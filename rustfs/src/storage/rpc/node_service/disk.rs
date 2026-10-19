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

use super::{LocalMutationTarget, NodeService};
use crate::storage::storage_api::ecstore_disk::{
    QUOTA_MUTATION_FENCE_METADATA_SUFFIX, RUSTFS_META_BUCKET, is_quota_mutation_fence_path,
};
use crate::storage::storage_api::rpc_consumer::node_service::{
    BatchReadVersionReq, BatchReadVersionResp, ConditionalFileUpdate, DeleteOptions, DiskError, DiskInfoOptions,
    FileInfoVersions, GuardedDeleteVersionsPayload, ReadMultipleReq, ReadMultipleResp, ReadOptions, StorageDiskRpcExt as _,
    UpdateMetadataOpts, validate_batch_read_version_item_count,
};
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use crate::storage::storage_api::{
    DELETE_RECOVERY_PROTOCOL_VERSION, DeleteRecoveryRpcOperation, DeleteRecoveryRpcReply, LegacyFileWrite, LegacyMetadataWrite,
    LegacyPartMutation, LegacyPartOperation, LegacyPathDelete, LegacyPathDeleteOperation, LegacyRenameData, LegacyRenameFile,
    PartTransactionAction, RenameDataResp, SnapshotLeaseToken, verify_tonic_canonical_body_digest,
    verify_tonic_mutation_body_digest,
};
use bytes::Bytes;
use rustfs_filemeta::FileInfo;
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_MSGPACK_CODEC_JSON, INTERNODE_MSGPACK_CODEC_MSGPACK, INTERNODE_MSGPACK_DIRECTION_REQUEST,
    INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION, INTERNODE_OPERATION_GRPC_COMPARE_AND_UPDATE_FILE,
    INTERNODE_OPERATION_GRPC_READ_ALL, INTERNODE_OPERATION_GRPC_READ_VERSION, INTERNODE_OPERATION_GRPC_WRITE_ALL,
    INTERNODE_STAGE_BATCH_READ_VERSION_DISK_READ, INTERNODE_STAGE_BATCH_READ_VERSION_REQUEST_DECODE,
    INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_JSON_ENCODE, INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_MSGPACK_ENCODE,
    INTERNODE_STAGE_READ_VERSION_DISK_READ, INTERNODE_STAGE_READ_VERSION_REQUEST_DECODE,
    INTERNODE_STAGE_READ_VERSION_RESPONSE_JSON_ENCODE, INTERNODE_STAGE_READ_VERSION_RESPONSE_MSGPACK_ENCODE,
    INTERNODE_TRANSPORT_BACKEND_GRPC, global_internode_metrics,
};
use rustfs_protos::proto_gen::node_service::*;
use serde::de::DeserializeOwned;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::debug;
use uuid::Uuid;

#[cfg(feature = "e2e-test-hooks")]
fn startup_cas_rename_observation(
    target: &LocalMutationTarget,
    request: &RenameDataRequest,
    file_info: &FileInfo,
) -> Option<serde_json::Value> {
    use sha2::{Digest, Sha256};
    if request.dst_volume != ".rustfs.sys" || !matches!(request.dst_path.as_str(), "pool.bin" | "pool.bin.identity") {
        return None;
    }
    let nonce = uuid::Uuid::parse_str(&std::env::var("RUSTFS_E2E_STARTUP_CAS_NONCE").ok()?).ok()?;
    let body = rustfs_protos::canonical_rename_data_request_body(request).ok()?;
    Some(serde_json::json!({
        "kind": "receiver", "nonce": nonce, "pid": std::process::id(),
        "target": match target { LocalMutationTarget::Ready(_) => "ready", LocalMutationTarget::Bootstrap(_) => "bootstrap", LocalMutationTarget::Unbound => "unbound" },
        "disk": request.disk, "src_volume": request.src_volume, "src_path": request.src_path,
        "dst_volume": request.dst_volume, "dst_path": request.dst_path,
        "body_sha256": rustfs_utils::crypto::hex(Sha256::digest(body)),
        "etag": file_info.metadata.get("etag"),
        "mod_time": file_info.mod_time.map(|time| time.unix_timestamp_nanos().to_string()),
    }))
}

impl LocalMutationTarget {
    async fn recover_local_delete_transaction(
        &self,
        disk_ref: &str,
        volume: &str,
        operation: DeleteRecoveryRpcOperation,
    ) -> Result<DeleteRecoveryRpcReply, DiskError> {
        match self {
            Self::Ready(store) => store.recover_local_delete_transaction(disk_ref, volume, operation).await,
            Self::Bootstrap(target) => target.recover_local_delete_transaction(disk_ref, volume, operation).await,
            Self::Unbound => Err(DiskError::other("target disk instance is unavailable")),
        }
    }

    async fn guarded_local_delete_versions(
        &self,
        disk_ref: &str,
        volume: &str,
        payload: GuardedDeleteVersionsPayload,
    ) -> Result<Vec<Option<DiskError>>, DiskError> {
        match self {
            Self::Ready(store) => store.guarded_local_delete_versions(disk_ref, volume, payload).await,
            Self::Bootstrap(target) => target.guarded_local_delete_versions(disk_ref, volume, payload).await,
            Self::Unbound => Err(DiskError::other("target disk instance is unavailable")),
        }
    }

    pub(crate) async fn find_mutation_disk(&self, disk_ref: &str) -> Option<super::DiskStore> {
        match self {
            Self::Ready(store) => store.local_mutation_disk(disk_ref).await.ok(),
            Self::Bootstrap(target) => target.local_mutation_disk(disk_ref).await.ok(),
            Self::Unbound => None,
        }
    }

    pub(crate) async fn reject_legacy_artifact_mutation(&self, volume: &str) -> Result<(), Status> {
        use crate::storage::storage_api::ecstore_bucket::utils::{has_bad_path_component, is_meta_bucketname};
        let allowed = match self {
            Self::Ready(store) => store.reject_legacy_artifact_mutation(volume).await.is_ok(),
            Self::Bootstrap(_) | Self::Unbound => !has_bad_path_component(volume) && is_meta_bucketname(volume),
        };
        if allowed {
            Ok(())
        } else {
            Err(Status::permission_denied("legacy artifact mutation is not permitted"))
        }
    }

    async fn begin_legacy_local_rename_data(
        &self,
        disk_ref: &str,
        source: (&str, &str),
        fi: &FileInfo,
        destination: (&str, &str),
    ) -> Result<LegacyRenameData, DiskError> {
        match self {
            Self::Ready(store) => store.begin_legacy_local_rename_data(disk_ref, source, fi, destination).await,
            Self::Bootstrap(target) => target.begin_legacy_local_rename_data(disk_ref, source, fi, destination).await,
            Self::Unbound => Err(DiskError::other("target disk instance is unavailable")),
        }
    }

    async fn rename_local_data(
        &self,
        disk_ref: &str,
        source: (&str, &str),
        fi: &FileInfo,
        destination: (&str, &str),
        scanner_token: Option<Uuid>,
        bucket_incarnation: Option<Uuid>,
    ) -> Result<RenameDataResp, DiskError> {
        match self {
            Self::Ready(store) => {
                if let Some(expected) = bucket_incarnation {
                    return store
                        .rename_local_data_at_incarnation(disk_ref, source, fi, destination, expected)
                        .await;
                }
                store
                    .rename_local_data(disk_ref, source, fi, destination, scanner_token)
                    .await
            }
            Self::Bootstrap(target) => {
                if bucket_incarnation.is_some() {
                    return Err(DiskError::other("incarnation-bound rename requires a ready storage instance"));
                }
                target
                    .rename_local_data(disk_ref, source, fi, destination, scanner_token)
                    .await
            }
            Self::Unbound => Err(DiskError::other("target disk instance is unavailable")),
        }
    }

    async fn begin_legacy_local_metadata_mutation(
        &self,
        disk_ref: &str,
        volume: &str,
        path: &str,
    ) -> Result<LegacyMetadataWrite, DiskError> {
        match self {
            Self::Ready(store) => store.begin_legacy_local_metadata_mutation(disk_ref, volume, path).await,
            Self::Bootstrap(target) => target.begin_legacy_local_metadata_mutation(disk_ref, volume, path).await,
            Self::Unbound => Err(DiskError::other("target disk instance is unavailable")),
        }
    }

    async fn begin_legacy_local_part_mutation(
        &self,
        disk_ref: &str,
        source: Option<(&str, &str)>,
        destination: (&str, &str),
        operation: LegacyPartOperation,
    ) -> Result<LegacyPartMutation, DiskError> {
        match self {
            Self::Ready(store) => {
                store
                    .begin_legacy_local_part_mutation(disk_ref, source, destination, operation)
                    .await
            }
            Self::Bootstrap(target) => {
                target
                    .begin_legacy_local_part_mutation(disk_ref, source, destination, operation)
                    .await
            }
            Self::Unbound => Err(DiskError::other("target disk instance is unavailable")),
        }
    }

    async fn begin_legacy_local_rename_file(
        &self,
        disk_ref: &str,
        source: (&str, &str),
        destination: (&str, &str),
        durable: bool,
    ) -> Result<LegacyRenameFile, DiskError> {
        match self {
            Self::Ready(store) => {
                store
                    .begin_legacy_local_rename_file(disk_ref, source, destination, durable)
                    .await
            }
            Self::Bootstrap(target) => {
                target
                    .begin_legacy_local_rename_file(disk_ref, source, destination, durable)
                    .await
            }
            Self::Unbound => Err(DiskError::other("target disk instance is unavailable")),
        }
    }

    pub(crate) async fn begin_legacy_local_file_write(
        &self,
        disk: &super::DiskStore,
        volume: &str,
        external_guard: Option<Arc<dyn Send + Sync>>,
    ) -> Result<LegacyFileWrite, DiskError> {
        match self {
            Self::Ready(store) => store.begin_legacy_local_file_write(disk, volume, external_guard).await,
            Self::Bootstrap(target) => target.begin_legacy_local_file_write(disk, volume, external_guard).await,
            Self::Unbound => Err(DiskError::other("cannot find disk")),
        }
    }

    async fn begin_legacy_local_delete_versions(&self, disk_ref: &str, volume: &str) -> Result<LegacyFileWrite, DiskError> {
        match self {
            Self::Ready(store) => store.begin_legacy_local_delete_versions(disk_ref, volume).await,
            Self::Bootstrap(target) => target.begin_legacy_local_delete_versions(disk_ref, volume).await,
            Self::Unbound => Err(DiskError::other("target disk instance is unavailable")),
        }
    }

    async fn begin_legacy_local_path_delete(
        &self,
        disk_ref: &str,
        volume: &str,
        operation: LegacyPathDeleteOperation,
        external_guard: Option<Arc<dyn Send + Sync>>,
    ) -> Result<LegacyPathDelete, DiskError> {
        match self {
            Self::Ready(store) => {
                store
                    .begin_legacy_local_path_delete(disk_ref, volume, operation, external_guard)
                    .await
            }
            Self::Bootstrap(target) => {
                target
                    .begin_legacy_local_path_delete(disk_ref, volume, operation, external_guard)
                    .await
            }
            Self::Unbound => Err(DiskError::other("cannot find disk")),
        }
    }
    async fn begin_legacy_local_residual_version_delete(
        &self,
        disk_ref: &str,
        volume: &str,
        path: &str,
        undo_write: bool,
    ) -> Result<LegacyMetadataWrite, DiskError> {
        match self {
            Self::Ready(store) => {
                store
                    .begin_legacy_local_residual_version_delete(disk_ref, volume, path, undo_write)
                    .await
            }
            Self::Bootstrap(target) => {
                target
                    .begin_legacy_local_residual_version_delete(disk_ref, volume, path, undo_write)
                    .await
            }
            Self::Unbound => Err(DiskError::other(if undo_write {
                "target disk instance is unavailable"
            } else {
                "cannot find disk"
            })),
        }
    }
}

/// Initial capacity hint (bytes) for typical small msgpack requests and responses.
const MSGPACK_ENCODE_CAPACITY_HINT: usize = 512;
const FILE_INFO_MSGPACK_ENCODE_CAPACITY_HINT: usize = 1024;
const SNAPSHOT_LEASE_PROTOCOL_VERSION: u32 = 1;

fn rejects_legacy_delete_transaction(volume: &str, opts: &DeleteOptions) -> bool {
    !(volume == ".rustfs.sys"
        || volume.starts_with(".rustfs.sys/")
        || volume == ".minio.sys"
        || volume.starts_with(".minio.sys/"))
        && opts.old_data_dir.is_some()
        && !opts.recoverable_delete
        && !opts.undo_write
}

fn snapshot_lease_response(result: Result<SnapshotLeaseToken, DiskError>) -> Response<SnapshotLeaseResponse> {
    match result {
        Ok(token) => Response::new(SnapshotLeaseResponse {
            success: true,
            token: token.as_bytes().to_vec().into(),
            protocol_version: SNAPSHOT_LEASE_PROTOCOL_VERSION,
            error: None,
        }),
        Err(err) => Response::new(SnapshotLeaseResponse {
            success: false,
            token: Bytes::new(),
            protocol_version: SNAPSHOT_LEASE_PROTOCOL_VERSION,
            error: Some(err.into()),
        }),
    }
}

pub(super) struct DecodedRpcPayload<T> {
    pub(super) value: T,
    pub(super) from_msgpack: bool,
}

fn decode_msgpack_or_json<T: DeserializeOwned>(
    binary: &[u8],
    json: &str,
    value_name: &'static str,
) -> std::result::Result<T, DiskError> {
    Ok(decode_msgpack_or_json_with_source(binary, json, value_name)?.value)
}

fn decode_msgpack_or_json_with_source<T: DeserializeOwned>(
    binary: &[u8],
    json: &str,
    value_name: &'static str,
) -> std::result::Result<DecodedRpcPayload<T>, DiskError> {
    if !binary.is_empty() {
        let mut deserializer = rmp_serde::Deserializer::new(Cursor::new(binary));
        return match T::deserialize(&mut deserializer) {
            Ok(value) => {
                global_internode_metrics().record_msgpack_json_decode(
                    INTERNODE_MSGPACK_DIRECTION_REQUEST,
                    value_name,
                    INTERNODE_MSGPACK_CODEC_MSGPACK,
                );
                Ok(DecodedRpcPayload {
                    value,
                    from_msgpack: true,
                })
            }
            Err(err) => {
                global_internode_metrics().record_msgpack_json_decode_error(
                    INTERNODE_MSGPACK_DIRECTION_REQUEST,
                    value_name,
                    INTERNODE_MSGPACK_CODEC_MSGPACK,
                );
                Err(DiskError::other(format!("decode {value_name} msgpack failed: {err}")))
            }
        };
    }

    // The msgpack payload was absent, so fall back to the JSON compatibility field. This branch
    // must read zero across a release window before the redundant JSON fields can be dropped (P2).
    global_internode_metrics().record_msgpack_json_fallback(INTERNODE_MSGPACK_DIRECTION_REQUEST, value_name);
    match serde_json::from_str(json) {
        Ok(value) => {
            global_internode_metrics().record_msgpack_json_decode(
                INTERNODE_MSGPACK_DIRECTION_REQUEST,
                value_name,
                INTERNODE_MSGPACK_CODEC_JSON,
            );
            Ok(DecodedRpcPayload {
                value,
                from_msgpack: false,
            })
        }
        Err(err) => {
            global_internode_metrics().record_msgpack_json_decode_error(
                INTERNODE_MSGPACK_DIRECTION_REQUEST,
                value_name,
                INTERNODE_MSGPACK_CODEC_JSON,
            );
            Err(DiskError::other(format!("decode {value_name} failed: {err}")))
        }
    }
}

fn encode_msgpack_with_capacity<T: serde::Serialize>(
    value: &T,
    value_name: &str,
    capacity: usize,
) -> std::result::Result<Vec<u8>, DiskError> {
    let mut serializer = rmp_serde::Serializer::new(Vec::with_capacity(capacity));
    value
        .serialize(&mut serializer)
        .map_err(|err| DiskError::other(format!("encode {value_name} msgpack failed: {err}")))?;
    Ok(serializer.into_inner())
}

fn encode_msgpack<T: serde::Serialize>(value: &T, value_name: &str) -> std::result::Result<Vec<u8>, DiskError> {
    encode_msgpack_with_capacity(value, value_name, MSGPACK_ENCODE_CAPACITY_HINT)
}

fn encode_file_info_msgpack(value: &FileInfo) -> std::result::Result<Vec<u8>, DiskError> {
    encode_msgpack_with_capacity(value, "FileInfo", FILE_INFO_MSGPACK_ENCODE_CAPACITY_HINT)
}

fn encode_delete_versions_errors(disk_errors: Vec<Option<DiskError>>) -> (Vec<String>, Vec<Error>) {
    let mut errors = Vec::with_capacity(disk_errors.len());
    let mut item_errors = Vec::with_capacity(disk_errors.len());
    for error in disk_errors {
        match error {
            Some(error) => {
                let code = match &error {
                    DiskError::Io(source) if source.kind() == std::io::ErrorKind::NotFound => DiskError::FileNotFound.to_u32(),
                    _ => error.to_u32(),
                };
                let error_info = error.to_string();
                errors.push(error_info.clone());
                item_errors.push(Error { code, error_info });
            }
            None => {
                errors.push(String::new());
                item_errors.push(Error::default());
            }
        }
    }
    (errors, item_errors)
}

fn encode_msgpack_named<T: serde::Serialize>(value: &T, value_name: &str) -> std::result::Result<Vec<u8>, DiskError> {
    let mut serializer = rmp_serde::Serializer::new(Vec::with_capacity(MSGPACK_ENCODE_CAPACITY_HINT)).with_struct_map();
    value
        .serialize(&mut serializer)
        .map_err(|err| DiskError::other(format!("encode {value_name} named msgpack failed: {err}")))?;
    Ok(serializer.into_inner())
}

/// Enforce the signature-bound canonical body digest on a mutating disk RPC (backlog#1327).
///
/// Digest-bearing requests are verified against the canonical bytes rebuilt from the received
/// wire fields — which cover both the msgpack `_bin` payloads and their JSON compatibility
/// copies, so tampering with either encoding (or stripping `_bin` to force the JSON fallback
/// decode) is rejected. Digestless requests fall back per
/// `RUSTFS_INTERNODE_RPC_BODY_DIGEST_STRICT` (default: accept + convergence counter).
fn verify_disk_mutation_digest<T>(
    request: &Request<T>,
    canonical_body: std::result::Result<Vec<u8>, std::num::TryFromIntError>,
    op: &'static str,
) -> std::result::Result<(), Status> {
    let canonical_body =
        canonical_body.map_err(|_| Status::invalid_argument(format!("{op} request length cannot be represented")))?;
    verify_tonic_mutation_body_digest(request, &canonical_body)
        .map_err(|err| Status::permission_denied(format!("{op} authentication failed: {err}")))
}

pub(super) fn verify_quota_mutation_digest<T>(
    request: &Request<T>,
    canonical_body: std::result::Result<Vec<u8>, std::num::TryFromIntError>,
) -> Result<String, Status> {
    let values = request.metadata().get_all(rustfs_protos::QUOTA_MUTATION_TOPOLOGY_HEADER);
    let mut values = values.iter();
    let topology = values
        .next()
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .ok_or_else(|| Status::permission_denied("quota mutation requires a valid topology header"))?;
    if values.next().is_some() {
        return Err(Status::permission_denied("quota mutation requires exactly one topology header"));
    }
    let body = canonical_body.map_err(|_| Status::invalid_argument("quota mutation request length cannot be represented"))?;
    let body = rustfs_protos::canonical_quota_mutation_request_body(topology, &body)
        .map_err(|_| Status::invalid_argument("quota mutation envelope length cannot be represented"))?;
    verify_tonic_canonical_body_digest(request, &body)
        .map_err(|_| Status::permission_denied("quota mutation requires topology-bound v2 authentication"))?;
    Ok(topology.to_owned())
}

/// JSON compatibility string for a dual-encoded response field. Returns an empty string when
/// msgpack-only mode and its explicit fleet confirmation guard are both enabled, or when
/// request-local `_bin` payloads prove the caller can consume the paired response `_bin` field,
/// while legacy JSON-only callers still need the compatibility response string during rollout.
/// The paired `_bin` field is always sent.
fn compat_response_json<T: serde::Serialize>(
    value: &T,
    request_had_msgpack_payload: bool,
) -> std::result::Result<String, serde_json::Error> {
    if request_had_msgpack_payload || rustfs_protos::internode_rpc_msgpack_only() {
        return Ok(String::new());
    }
    serde_json::to_string(value)
}

fn encode_read_multiple_response_payloads(
    read_multiple_resps: &[ReadMultipleResp],
) -> std::result::Result<(Vec<String>, Vec<Bytes>), DiskError> {
    let mut read_multiple_resps_json = Vec::with_capacity(read_multiple_resps.len());
    let mut read_multiple_resps_bin = Vec::with_capacity(read_multiple_resps.len());

    for read_multiple_resp in read_multiple_resps {
        read_multiple_resps_json.push(
            compat_response_json(read_multiple_resp, false)
                .map_err(|err| DiskError::other(format!("encode ReadMultipleResp json failed: {err}")))?,
        );
        read_multiple_resps_bin.push(Bytes::from(encode_msgpack(read_multiple_resp, "ReadMultipleResp")?));
    }

    Ok((read_multiple_resps_json, read_multiple_resps_bin))
}

fn internode_stage_timer(attribution_enabled: bool) -> Option<Instant> {
    attribution_enabled.then(Instant::now)
}

fn record_read_version_stage(stage: &'static str, started_at: Option<Instant>) {
    if let Some(started_at) = started_at {
        global_internode_metrics().record_stage_duration_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_READ_VERSION,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
            stage,
            started_at.elapsed(),
        );
    }
}

fn record_batch_read_version_stage(stage: &'static str, started_at: Option<Instant>) {
    if let Some(started_at) = started_at {
        global_internode_metrics().record_stage_duration_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
            stage,
            started_at.elapsed(),
        );
    }
}

fn encode_batch_read_version_response_payloads(
    batch_read_version_resps: &[BatchReadVersionResp],
    request_decoded_from_msgpack: bool,
) -> std::result::Result<(Vec<String>, Vec<Bytes>), DiskError> {
    let attribution_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
    let mut batch_read_version_resps_json = Vec::with_capacity(batch_read_version_resps.len());
    let json_encode_started = internode_stage_timer(attribution_enabled);
    for batch_read_version_resp in batch_read_version_resps {
        batch_read_version_resps_json.push(
            compat_response_json(batch_read_version_resp, request_decoded_from_msgpack)
                .map_err(|err| DiskError::other(format!("encode BatchReadVersionResp json failed: {err}")))?,
        );
    }
    record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_JSON_ENCODE, json_encode_started);

    let mut batch_read_version_resps_bin = Vec::with_capacity(batch_read_version_resps.len());
    let msgpack_encode_started = internode_stage_timer(attribution_enabled);
    for batch_read_version_resp in batch_read_version_resps {
        batch_read_version_resps_bin.push(Bytes::from(encode_msgpack_with_capacity(
            batch_read_version_resp,
            "BatchReadVersionResp",
            FILE_INFO_MSGPACK_ENCODE_CAPACITY_HINT,
        )?));
    }
    record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_MSGPACK_ENCODE, msgpack_encode_started);

    Ok((batch_read_version_resps_json, batch_read_version_resps_bin))
}

pub(super) fn decode_rename_data_request_file_info(
    binary: &[u8],
    json: &str,
) -> std::result::Result<DecodedRpcPayload<FileInfo>, DiskError> {
    decode_msgpack_or_json_with_source(binary, json, "FileInfo")
}

pub(super) fn encode_rename_data_response_payloads(
    rename_data_resp: &RenameDataResp,
    request_decoded_from_msgpack: bool,
) -> std::result::Result<(String, Vec<u8>), DiskError> {
    let rename_data_resp_json = compat_response_json(rename_data_resp, request_decoded_from_msgpack)
        .map_err(|err| DiskError::other(format!("encode RenameDataResp json failed: {err}")))?;
    let rename_data_resp_bin = encode_msgpack_named(rename_data_resp, "RenameDataResp")?;
    Ok((rename_data_resp_json, rename_data_resp_bin))
}

impl NodeService {
    pub(super) fn quota_store_for_topology<'a>(
        &self,
        target: &'a LocalMutationTarget,
        coordinator_topology: &str,
    ) -> Result<&'a Arc<super::ECStore>, Status> {
        let LocalMutationTarget::Ready(store) = target else {
            return Err(Status::failed_precondition("quota mutation requires a ready storage instance"));
        };
        let mut cache = self
            .quota_topology_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let identity = Arc::downgrade(store);
        if !cache.as_ref().is_some_and(|entry| entry.store.ptr_eq(&identity)) {
            // Instance endpoints are immutable once initialized. Keep one weak
            // identity so a replacement instance cannot inherit this layout,
            // even when its deployment or disk UUIDs are unchanged.
            let endpoints = store
                .instance_endpoints()
                .ok_or_else(|| Status::failed_precondition("quota mutation target topology is unavailable"))?;
            let fingerprint = super::heal::heal_topology_fingerprint(&endpoints)
                .map_err(|_| Status::failed_precondition("quota mutation target topology is invalid"))?;
            #[cfg(test)]
            self.quota_topology_computations
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            *cache = Some(super::QuotaTopologyCacheEntry {
                store: identity,
                fingerprint,
            });
        }
        if cache.as_ref().is_none_or(|entry| entry.fingerprint != coordinator_topology) {
            return Err(Status::failed_precondition(
                "quota mutation coordinator topology does not match the target",
            ));
        }
        Ok(store)
    }

    pub(super) async fn handle_acquire_snapshot_lease(
        &self,
        request: Request<SnapshotLeaseRequest>,
        quota_fence: bool,
    ) -> Result<Response<SnapshotLeaseResponse>, Status> {
        let body = rustfs_protos::canonical_snapshot_lease_request_body(request.get_ref());
        let quota_store = if quota_fence {
            let topology = verify_quota_mutation_digest(&request, body)?;
            if request.get_ref().volume != RUSTFS_META_BUCKET || !is_quota_mutation_fence_path(&request.get_ref().path) {
                return Err(Status::invalid_argument("quota fence requires its internal mutation fence path"));
            }
            Some(Arc::clone(self.quota_store_for_topology(&self.local_mutation_target(), &topology)?))
        } else {
            verify_disk_mutation_digest(&request, body, "acquire_snapshot_lease")?;
            None
        };
        let request = request.into_inner();
        let disk = match quota_store {
            Some(store) => store.quota_mutation_fence_disk(&request.disk).await,
            None => self
                .find_disk(&request.disk)
                .await
                .ok_or_else(|| DiskError::other("cannot find disk")),
        };
        let result = match disk {
            Ok(disk) => disk.acquire_snapshot_lease(&request.volume, &request.path).await,
            Err(err) => Err(err),
        };
        Ok(snapshot_lease_response(result))
    }

    pub(super) async fn handle_renew_snapshot_lease(
        &self,
        request: Request<SnapshotLeaseRenewRequest>,
    ) -> Result<Response<SnapshotLeaseResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_snapshot_lease_renew_request_body(request.get_ref()),
            "renew_snapshot_lease",
        )?;
        let request = request.into_inner();
        let token =
            SnapshotLeaseToken::from_slice(&request.token).map_err(|_| Status::invalid_argument("invalid lease token"))?;
        let result = match self.find_disk(&request.disk).await {
            Some(disk) => disk.renew_snapshot_lease(&request.volume, &request.path, token).await,
            None => Err(DiskError::other("cannot find disk")),
        };
        Ok(snapshot_lease_response(result))
    }

    pub(super) async fn handle_release_snapshot_lease(
        &self,
        request: Request<SnapshotLeaseReleaseRequest>,
        quota_fence: bool,
    ) -> Result<Response<SnapshotLeaseMutationResponse>, Status> {
        let body = rustfs_protos::canonical_snapshot_lease_release_request_body(request.get_ref());
        let quota_store = if quota_fence {
            let topology = verify_quota_mutation_digest(&request, body)?;
            if request.get_ref().volume != RUSTFS_META_BUCKET || !is_quota_mutation_fence_path(&request.get_ref().path) {
                return Err(Status::invalid_argument("quota fence requires its internal mutation fence path"));
            }
            Some(Arc::clone(self.quota_store_for_topology(&self.local_mutation_target(), &topology)?))
        } else {
            verify_disk_mutation_digest(&request, body, "release_snapshot_lease")?;
            None
        };
        let request = request.into_inner();
        let token = if request.token.as_ref() == SnapshotLeaseToken::revoke_all().as_bytes() {
            SnapshotLeaseToken::revoke_all()
        } else {
            SnapshotLeaseToken::from_slice(&request.token).map_err(|_| Status::invalid_argument("invalid lease token"))?
        };
        let disk = match quota_store {
            Some(store) => store.quota_mutation_fence_disk(&request.disk).await,
            None => self
                .find_disk(&request.disk)
                .await
                .ok_or_else(|| DiskError::other("cannot find disk")),
        };
        let result = match disk {
            Ok(disk) => disk.release_snapshot_lease(&request.volume, &request.path, token).await,
            Err(err) => Err(err),
        };
        match result {
            Ok(()) => Ok(Response::new(SnapshotLeaseMutationResponse {
                success: true,
                error: None,
            })),
            Err(err) => Ok(Response::new(SnapshotLeaseMutationResponse {
                success: false,
                error: Some(err.into()),
            })),
        }
    }

    pub(super) async fn handle_disk_info(&self, request: Request<DiskInfoRequest>) -> Result<Response<DiskInfoResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let opts = match serde_json::from_str::<DiskInfoOptions>(&request.opts) {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(Response::new(DiskInfoResponse {
                        success: false,
                        disk_info: "".to_string(),
                        error: Some(DiskError::other(format!("decode DiskInfoOptions failed: {err}")).into()),
                    }));
                }
            };
            match disk.disk_info(&opts).await {
                Ok(disk_info) => match serde_json::to_string(&disk_info) {
                    Ok(disk_info) => Ok(Response::new(DiskInfoResponse {
                        success: true,
                        disk_info,
                        error: None,
                    })),
                    Err(err) => Ok(Response::new(DiskInfoResponse {
                        success: false,
                        disk_info: "".to_string(),
                        error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                    })),
                },
                Err(err) => Ok(Response::new(DiskInfoResponse {
                    success: false,
                    disk_info: "".to_string(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DiskInfoResponse {
                success: false,
                disk_info: "".to_string(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_delete_volume_request_body(request.get_ref()),
            "delete_volume",
        )?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            // Volume deletion cannot publish an artifact version. It follows
            // the coordinator's lifecycle-write path, including metadata-less
            // legacy buckets; object publication admission does not apply.
            match disk.delete_volume(&request.volume, request.force).await {
                Ok(_) => Ok(Response::new(DeleteVolumeResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(DeleteVolumeResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DeleteVolumeResponse {
                success: false,
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_read_multiple(
        &self,
        request: Request<ReadMultipleRequest>,
    ) -> Result<Response<ReadMultipleResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let read_multiple_req = match decode_msgpack_or_json::<ReadMultipleReq>(
                &request.read_multiple_req_bin,
                &request.read_multiple_req,
                "ReadMultipleReq",
            ) {
                Ok(read_multiple_req) => read_multiple_req,
                Err(err) => {
                    return Ok(Response::new(ReadMultipleResponse {
                        success: false,
                        read_multiple_resps: Vec::new(),
                        read_multiple_resps_bin: Vec::new(),
                        error: Some(DiskError::other(format!("decode ReadMultipleReq failed: {err}")).into()),
                    }));
                }
            };
            match disk.read_multiple(read_multiple_req).await {
                Ok(read_multiple_resps) => {
                    let (read_multiple_resps, read_multiple_resps_bin) =
                        match encode_read_multiple_response_payloads(&read_multiple_resps) {
                            Ok(payloads) => payloads,
                            Err(err) => {
                                return Ok(Response::new(ReadMultipleResponse {
                                    success: false,
                                    read_multiple_resps: Vec::new(),
                                    read_multiple_resps_bin: Vec::new(),
                                    error: Some(err.into()),
                                }));
                            }
                        };

                    Ok(Response::new(ReadMultipleResponse {
                        success: true,
                        read_multiple_resps,
                        read_multiple_resps_bin,
                        error: None,
                    }))
                }
                Err(err) => Ok(Response::new(ReadMultipleResponse {
                    success: false,
                    read_multiple_resps: Vec::new(),
                    read_multiple_resps_bin: Vec::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadMultipleResponse {
                success: false,
                read_multiple_resps: Vec::new(),
                read_multiple_resps_bin: Vec::new(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_batch_read_version(
        &self,
        request: Request<BatchReadVersionRequest>,
    ) -> Result<Response<BatchReadVersionResponse>, Status> {
        let attribution_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        let request = request.into_inner();
        if attribution_enabled {
            let metrics = global_internode_metrics();
            metrics.record_incoming_request_for_operation_and_backend(
                INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION,
                INTERNODE_TRANSPORT_BACKEND_GRPC,
            );
            metrics.record_recv_bytes_for_operation_and_backend(
                INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION,
                INTERNODE_TRANSPORT_BACKEND_GRPC,
                request
                    .disk
                    .len()
                    .saturating_add(request.batch_read_version_req.len())
                    .saturating_add(request.batch_read_version_req_bin.len()),
            );
        }
        if let Some(disk) = self.find_disk(&request.disk).await {
            let decode_started = internode_stage_timer(attribution_enabled);
            let decoded_batch_read_version_req: DecodedRpcPayload<BatchReadVersionReq> = match decode_msgpack_or_json_with_source(
                &request.batch_read_version_req_bin,
                &request.batch_read_version_req,
                "BatchReadVersionReq",
            ) {
                Ok(batch_read_version_req) => {
                    record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_REQUEST_DECODE, decode_started);
                    batch_read_version_req
                }
                Err(err) => {
                    record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_REQUEST_DECODE, decode_started);
                    return Ok(Response::new(BatchReadVersionResponse {
                        success: false,
                        batch_read_version_resps: Vec::new(),
                        batch_read_version_resps_bin: Vec::new(),
                        error: Some(DiskError::other(format!("decode BatchReadVersionReq failed: {err}")).into()),
                    }));
                }
            };
            let request_decoded_from_msgpack = decoded_batch_read_version_req.from_msgpack;
            let batch_read_version_req = decoded_batch_read_version_req.value;

            if let Err(err) = validate_batch_read_version_item_count(batch_read_version_req.items.len()) {
                return Ok(Response::new(BatchReadVersionResponse {
                    success: false,
                    batch_read_version_resps: Vec::new(),
                    batch_read_version_resps_bin: Vec::new(),
                    error: Some(err.into()),
                }));
            }

            let disk_read_started = internode_stage_timer(attribution_enabled);
            match disk.batch_read_version(batch_read_version_req).await {
                Ok(batch_read_version_resps) => {
                    record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_DISK_READ, disk_read_started);
                    let (batch_read_version_resps, batch_read_version_resps_bin) =
                        match encode_batch_read_version_response_payloads(&batch_read_version_resps, request_decoded_from_msgpack)
                        {
                            Ok(payloads) => payloads,
                            Err(err) => {
                                return Ok(Response::new(BatchReadVersionResponse {
                                    success: false,
                                    batch_read_version_resps: Vec::new(),
                                    batch_read_version_resps_bin: Vec::new(),
                                    error: Some(err.into()),
                                }));
                            }
                        };

                    Ok(Response::new(BatchReadVersionResponse {
                        success: true,
                        batch_read_version_resps,
                        batch_read_version_resps_bin,
                        error: None,
                    }))
                }
                Err(err) => {
                    record_batch_read_version_stage(INTERNODE_STAGE_BATCH_READ_VERSION_DISK_READ, disk_read_started);
                    Ok(Response::new(BatchReadVersionResponse {
                        success: false,
                        batch_read_version_resps: Vec::new(),
                        batch_read_version_resps_bin: Vec::new(),
                        error: Some(err.into()),
                    }))
                }
            }
        } else {
            Ok(Response::new(BatchReadVersionResponse {
                success: false,
                batch_read_version_resps: Vec::new(),
                batch_read_version_resps_bin: Vec::new(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_guarded_delete_versions(
        &self,
        request: Request<GuardedDeleteVersionsRequest>,
    ) -> Result<Response<DeleteVersionsResponse>, Status> {
        if request.get_ref().payload.len() > rustfs_protos::DELETE_RECOVERY_MAX_PAYLOAD_BYTES {
            return Err(Status::resource_exhausted("guarded delete payload exceeds limit"));
        }
        let body = rustfs_protos::canonical_guarded_delete_versions_request_body(request.get_ref())
            .map_err(|_| Status::invalid_argument("invalid guarded delete body length"))?;
        verify_tonic_canonical_body_digest(&request, &body)
            .map_err(|_| Status::permission_denied("guarded delete requires body-bound v2 authentication"))?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        let disk = target
            .find_mutation_disk(&request.disk)
            .await
            .ok_or_else(|| Status::not_found("cannot find disk"))?;
        if request.probe_only {
            if !request.payload.is_empty() || !request.volume.is_empty() {
                return Err(Status::invalid_argument("guarded capability probe must have no mutation payload"));
            }
            if let Err(error) = disk.supports_guarded_delete_versions().await {
                return Ok(Response::new(DeleteVersionsResponse {
                    success: false,
                    errors: Vec::new(),
                    error: Some(error.into()),
                    item_errors: Vec::new(),
                }));
            }
            return Ok(Response::new(DeleteVersionsResponse {
                success: true,
                errors: Vec::new(),
                error: None,
                item_errors: Vec::new(),
            }));
        }
        let payload: GuardedDeleteVersionsPayload = rustfs_protos::decode_delete_recovery_payload(&request.payload)
            .map_err(|_| Status::invalid_argument("invalid guarded delete payload"))?;
        payload
            .validate()
            .map_err(|_| Status::invalid_argument("invalid guarded delete options or batch"))?;
        target.reject_legacy_artifact_mutation(&request.volume).await?;
        let result = target
            .guarded_local_delete_versions(&request.disk, &request.volume, payload)
            .await;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                return Ok(Response::new(DeleteVersionsResponse {
                    success: false,
                    errors: Vec::new(),
                    error: Some(error.into()),
                    item_errors: Vec::new(),
                }));
            }
        };
        let (errors, item_errors) = encode_delete_versions_errors(result);
        Ok(Response::new(DeleteVersionsResponse {
            success: true,
            errors,
            error: None,
            item_errors,
        }))
    }

    pub(super) async fn handle_delete_versions(
        &self,
        request: Request<DeleteVersionsRequest>,
    ) -> Result<Response<DeleteVersionsResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_delete_versions_request_body(request.get_ref()),
            "delete_versions",
        )?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            if request.versions.is_empty() {
                target.reject_legacy_artifact_mutation(&request.volume).await?;
            }
            let mut versions = Vec::with_capacity(request.versions.len());
            for (index, version) in request.versions.iter().enumerate() {
                let version_bin = request.versions_bin.get(index).map(|b| b.as_ref()).unwrap_or(&[]);
                match decode_msgpack_or_json::<FileInfoVersions>(version_bin, version, "FileInfoVersions") {
                    Ok(version) => versions.push(version),
                    Err(err) => {
                        return Ok(Response::new(DeleteVersionsResponse {
                            success: false,
                            errors: Vec::new(),
                            error: Some(DiskError::other(format!("decode FileInfoVersions failed: {err}")).into()),
                            item_errors: Vec::new(),
                        }));
                    }
                };
            }
            let opts = match decode_msgpack_or_json::<DeleteOptions>(&request.opts_bin, &request.opts, "DeleteOptions") {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(Response::new(DeleteVersionsResponse {
                        success: false,
                        errors: Vec::new(),
                        error: Some(DiskError::other(format!("decode DeleteOptions failed: {err}")).into()),
                        item_errors: Vec::new(),
                    }));
                }
            };

            if rejects_legacy_delete_transaction(&request.volume, &opts) {
                return Ok(Response::new(DeleteVersionsResponse {
                    success: false,
                    errors: Vec::new(),
                    error: Some(DiskError::UnsupportedDisk.into()),
                    item_errors: Vec::new(),
                }));
            }
            let result = if versions.is_empty() {
                disk.delete_versions(&request.volume, versions, opts).await
            } else {
                let write = match target
                    .begin_legacy_local_delete_versions(&request.disk, &request.volume)
                    .await
                {
                    Ok(write) => write,
                    Err(error) => {
                        return Ok(Response::new(DeleteVersionsResponse {
                            success: false,
                            errors: Vec::new(),
                            error: Some(error.into()),
                            item_errors: Vec::new(),
                        }));
                    }
                };
                target.reject_legacy_artifact_mutation(&request.volume).await?;
                write.delete_versions(versions, opts).await
            };
            let (errors, item_errors) = encode_delete_versions_errors(result);

            Ok(Response::new(DeleteVersionsResponse {
                success: true,
                errors,
                error: None,
                item_errors,
            }))
        } else {
            Ok(Response::new(DeleteVersionsResponse {
                success: false,
                errors: Vec::new(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
                item_errors: Vec::new(),
            }))
        }
    }

    pub(super) async fn handle_delete_version(
        &self,
        request: Request<DeleteVersionRequest>,
        require_marker_condition: bool,
    ) -> Result<Response<DeleteVersionResponse>, Status> {
        if !request.get_ref().bucket_incarnation_id.is_empty()
            && !request
                .metadata()
                .get("x-rustfs-content-sha256")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value != "UNSIGNED-PAYLOAD")
        {
            return Err(Status::permission_denied("incarnation-bound delete requires a body-bound digest"));
        }
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_delete_version_request_body(request.get_ref()),
            "delete_version",
        )?;
        if require_marker_condition
            && Uuid::from_slice(&request.get_ref().bucket_incarnation_id)
                .ok()
                .filter(|id| !id.is_nil())
                .is_none()
        {
            return Err(Status::invalid_argument("retired marker deletion requires a non-nil bucket incarnation"));
        }
        let request = request.into_inner();
        let target = self.local_mutation_target();
        let file_info = match decode_msgpack_or_json::<FileInfo>(&request.file_info_bin, &request.file_info, "FileInfo") {
            Ok(file_info) => file_info,
            Err(err) => {
                return Ok(Response::new(DeleteVersionResponse {
                    success: false,
                    raw_file_info: "".to_string(),
                    error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                }));
            }
        };
        let opts = match decode_msgpack_or_json::<DeleteOptions>(&request.opts_bin, &request.opts, "DeleteOptions") {
            Ok(opts) => opts,
            Err(err) => {
                return Ok(Response::new(DeleteVersionResponse {
                    success: false,
                    raw_file_info: "".to_string(),
                    error: Some(DiskError::other(format!("decode DeleteOptions failed: {err}")).into()),
                }));
            }
        };
        if rejects_legacy_delete_transaction(&request.volume, &opts) {
            return Ok(Response::new(DeleteVersionResponse {
                success: false,
                raw_file_info: String::new(),
                error: Some(DiskError::UnsupportedDisk.into()),
            }));
        }
        if require_marker_condition && opts.expected_delete_marker.is_none() {
            return Err(Status::invalid_argument("retired marker deletion requires a marker precondition"));
        }
        if opts.expected_delete_marker.is_some()
            && (request.force_del_marker
                || opts.undo_write
                || opts.undo_delete
                || opts.recursive
                || opts.immediate
                || opts.old_data_dir.is_some())
        {
            return Err(Status::invalid_argument(
                "retired marker preconditions cannot be combined with other mutations",
            ));
        }
        let result = if !request.bucket_incarnation_id.is_empty() {
            target.reject_legacy_artifact_mutation(&request.volume).await?;
            let expected = Uuid::from_slice(&request.bucket_incarnation_id)
                .ok()
                .filter(|id| !id.is_nil())
                .ok_or_else(|| Status::invalid_argument("bucket incarnation must be a non-nil UUID"))?;
            let LocalMutationTarget::Ready(store) = target else {
                return Err(Status::failed_precondition("bucket heal requires a ready storage instance"));
            };
            store
                .delete_local_version_at_incarnation(
                    &request.disk,
                    (&request.volume, &request.path),
                    file_info,
                    request.force_del_marker,
                    opts,
                    expected,
                )
                .await
        } else if opts.undo_write {
            if request.force_del_marker {
                target.reject_legacy_artifact_mutation(&request.volume).await?;
                Err(DiskError::other("undo_write cannot force a delete marker"))
            } else if opts.undo_delete && opts.old_data_dir.is_some() {
                match target
                    .begin_legacy_local_metadata_mutation(&request.disk, &request.volume, &request.path)
                    .await
                {
                    Ok(write) => {
                        target.reject_legacy_artifact_mutation(&request.volume).await?;
                        write.undo_delete(file_info, opts).await
                    }
                    Err(err) => Err(err),
                }
            } else if opts.undo_delete {
                match target
                    .begin_legacy_local_residual_version_delete(&request.disk, &request.volume, &request.path, true)
                    .await
                {
                    Ok(write) => {
                        target.reject_legacy_artifact_mutation(&request.volume).await?;
                        write.delete_version_residual(file_info, request.force_del_marker, opts).await
                    }
                    Err(err) => Err(err),
                }
            } else {
                match target
                    .begin_legacy_local_metadata_mutation(&request.disk, &request.volume, &request.path)
                    .await
                {
                    Ok(write) => {
                        target.reject_legacy_artifact_mutation(&request.volume).await?;
                        write.undo_write(file_info, opts).await
                    }
                    Err(err) => Err(err),
                }
            }
        } else if !opts.undo_delete && opts.old_data_dir.is_none() {
            match target
                .begin_legacy_local_metadata_mutation(&request.disk, &request.volume, &request.path)
                .await
            {
                Ok(write) => {
                    target.reject_legacy_artifact_mutation(&request.volume).await?;
                    write.delete_version(file_info, request.force_del_marker, opts).await
                }
                Err(err) => Err(err),
            }
        } else if !opts.undo_delete && opts.old_data_dir.is_some() {
            match target
                .begin_legacy_local_metadata_mutation(&request.disk, &request.volume, &request.path)
                .await
            {
                Ok(write) => {
                    target.reject_legacy_artifact_mutation(&request.volume).await?;
                    write.stage_delete_version(file_info, request.force_del_marker, opts).await
                }
                Err(err) => Err(err),
            }
        } else {
            match target
                .begin_legacy_local_residual_version_delete(&request.disk, &request.volume, &request.path, false)
                .await
            {
                Ok(write) => {
                    target.reject_legacy_artifact_mutation(&request.volume).await?;
                    write.delete_version_residual(file_info, request.force_del_marker, opts).await
                }
                Err(err) => Err(err),
            }
        };
        match result {
            Ok(raw_file_info) => match serde_json::to_string(&raw_file_info) {
                Ok(raw_file_info) => Ok(Response::new(DeleteVersionResponse {
                    success: true,
                    raw_file_info,
                    error: None,
                })),
                Err(err) => Ok(Response::new(DeleteVersionResponse {
                    success: false,
                    raw_file_info: "".to_string(),
                    error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                })),
            },
            Err(err) => Ok(Response::new(DeleteVersionResponse {
                success: false,
                raw_file_info: "".to_string(),
                error: Some(err.into()),
            })),
        }
    }

    pub(super) async fn handle_read_xl(&self, request: Request<ReadXlRequest>) -> Result<Response<ReadXlResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_xl(&request.volume, &request.path, request.read_data).await {
                Ok(raw_file_info) => {
                    let raw_file_info_json = compat_response_json(&raw_file_info, false);
                    let raw_file_info_bin = encode_msgpack(&raw_file_info, "RawFileInfo");
                    match (raw_file_info_json, raw_file_info_bin) {
                        (Ok(raw_file_info), Ok(raw_file_info_bin)) => Ok(Response::new(ReadXlResponse {
                            success: true,
                            raw_file_info,
                            raw_file_info_bin: raw_file_info_bin.into(),
                            error: None,
                        })),
                        (Err(err), _) => Ok(Response::new(ReadXlResponse {
                            success: false,
                            raw_file_info: String::new(),
                            raw_file_info_bin: Vec::new().into(),
                            error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                        })),
                        (_, Err(err)) => Ok(Response::new(ReadXlResponse {
                            success: false,
                            raw_file_info: String::new(),
                            raw_file_info_bin: Vec::new().into(),
                            error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                        })),
                    }
                }
                Err(err) => Ok(Response::new(ReadXlResponse {
                    success: false,
                    raw_file_info: String::new(),
                    raw_file_info_bin: Vec::new().into(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadXlResponse {
                success: false,
                raw_file_info: String::new(),
                raw_file_info_bin: Vec::new().into(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_read_version(
        &self,
        request: Request<ReadVersionRequest>,
    ) -> Result<Response<ReadVersionResponse>, Status> {
        let metrics = global_internode_metrics();
        let read_version_attribution_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        let request = request.into_inner();
        if read_version_attribution_enabled {
            metrics.record_incoming_request_for_operation_and_backend(
                INTERNODE_OPERATION_GRPC_READ_VERSION,
                INTERNODE_TRANSPORT_BACKEND_GRPC,
            );
            metrics.record_recv_bytes_for_operation_and_backend(
                INTERNODE_OPERATION_GRPC_READ_VERSION,
                INTERNODE_TRANSPORT_BACKEND_GRPC,
                request
                    .disk
                    .len()
                    .saturating_add(request.volume.len())
                    .saturating_add(request.path.len())
                    .saturating_add(request.version_id.len())
                    .saturating_add(request.opts.len())
                    .saturating_add(request.opts_bin.len()),
            );
        }
        if let Some(disk) = self.find_disk(&request.disk).await {
            let request_had_msgpack_payload = !request.opts_bin.is_empty();
            let decode_started = internode_stage_timer(read_version_attribution_enabled);
            let opts = match decode_msgpack_or_json::<ReadOptions>(&request.opts_bin, &request.opts, "ReadOptions") {
                Ok(options) => {
                    record_read_version_stage(INTERNODE_STAGE_READ_VERSION_REQUEST_DECODE, decode_started);
                    options
                }
                Err(err) => {
                    record_read_version_stage(INTERNODE_STAGE_READ_VERSION_REQUEST_DECODE, decode_started);
                    if read_version_attribution_enabled {
                        metrics.record_error_for_operation_and_backend(
                            INTERNODE_OPERATION_GRPC_READ_VERSION,
                            INTERNODE_TRANSPORT_BACKEND_GRPC,
                        );
                    }
                    return Ok(Response::new(ReadVersionResponse {
                        success: false,
                        file_info: String::new(),
                        file_info_bin: Vec::new().into(),
                        error: Some(DiskError::other(format!("decode ReadOptions failed: {err}")).into()),
                    }));
                }
            };
            let disk_read_started = internode_stage_timer(read_version_attribution_enabled);
            match disk
                .read_version("", &request.volume, &request.path, &request.version_id, &opts)
                .await
            {
                Ok(file_info) => {
                    record_read_version_stage(INTERNODE_STAGE_READ_VERSION_DISK_READ, disk_read_started);
                    let json_encode_started = internode_stage_timer(read_version_attribution_enabled);
                    let file_info_json = compat_response_json(&file_info, request_had_msgpack_payload);
                    record_read_version_stage(INTERNODE_STAGE_READ_VERSION_RESPONSE_JSON_ENCODE, json_encode_started);
                    let msgpack_encode_started = internode_stage_timer(read_version_attribution_enabled);
                    let file_info_bin = encode_file_info_msgpack(&file_info);
                    record_read_version_stage(INTERNODE_STAGE_READ_VERSION_RESPONSE_MSGPACK_ENCODE, msgpack_encode_started);
                    match (file_info_json, file_info_bin) {
                        (Ok(file_info), Ok(file_info_bin)) => {
                            if read_version_attribution_enabled {
                                metrics.record_sent_bytes_for_operation_and_backend(
                                    INTERNODE_OPERATION_GRPC_READ_VERSION,
                                    INTERNODE_TRANSPORT_BACKEND_GRPC,
                                    file_info.len().saturating_add(file_info_bin.len()),
                                );
                            }
                            Ok(Response::new(ReadVersionResponse {
                                success: true,
                                file_info,
                                file_info_bin: file_info_bin.into(),
                                error: None,
                            }))
                        }
                        (Err(err), _) => {
                            if read_version_attribution_enabled {
                                metrics.record_error_for_operation_and_backend(
                                    INTERNODE_OPERATION_GRPC_READ_VERSION,
                                    INTERNODE_TRANSPORT_BACKEND_GRPC,
                                );
                            }
                            Ok(Response::new(ReadVersionResponse {
                                success: false,
                                file_info: String::new(),
                                file_info_bin: Vec::new().into(),
                                error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                            }))
                        }
                        (_, Err(err)) => {
                            if read_version_attribution_enabled {
                                metrics.record_error_for_operation_and_backend(
                                    INTERNODE_OPERATION_GRPC_READ_VERSION,
                                    INTERNODE_TRANSPORT_BACKEND_GRPC,
                                );
                            }
                            Ok(Response::new(ReadVersionResponse {
                                success: false,
                                file_info: String::new(),
                                file_info_bin: Vec::new().into(),
                                error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                            }))
                        }
                    }
                }
                Err(err) => {
                    record_read_version_stage(INTERNODE_STAGE_READ_VERSION_DISK_READ, disk_read_started);
                    if read_version_attribution_enabled {
                        metrics.record_error_for_operation_and_backend(
                            INTERNODE_OPERATION_GRPC_READ_VERSION,
                            INTERNODE_TRANSPORT_BACKEND_GRPC,
                        );
                    }
                    Ok(Response::new(ReadVersionResponse {
                        success: false,
                        file_info: String::new(),
                        file_info_bin: Vec::new().into(),
                        error: Some(err.into()),
                    }))
                }
            }
        } else {
            if read_version_attribution_enabled {
                metrics.record_error_for_operation_and_backend(
                    INTERNODE_OPERATION_GRPC_READ_VERSION,
                    INTERNODE_TRANSPORT_BACKEND_GRPC,
                );
            }
            Ok(Response::new(ReadVersionResponse {
                success: false,
                file_info: String::new(),
                file_info_bin: Vec::new().into(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_write_metadata(
        &self,
        request: Request<WriteMetadataRequest>,
    ) -> Result<Response<WriteMetadataResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_write_metadata_request_body(request.get_ref()),
            "write_metadata",
        )?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        if !request.bucket_incarnation_id.is_empty() {
            target.reject_legacy_artifact_mutation(&request.volume).await?;
            let expected = Uuid::from_slice(&request.bucket_incarnation_id)
                .ok()
                .filter(|id| !id.is_nil())
                .ok_or_else(|| Status::invalid_argument("bucket incarnation must be a non-nil UUID"))?;
            let LocalMutationTarget::Ready(store) = target else {
                return Err(Status::failed_precondition("bucket heal requires a ready storage instance"));
            };
            let value = decode_msgpack_or_json::<FileInfo>(&request.file_info_bin, &request.file_info, "FileInfo")
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let result = store
                .write_local_metadata_at_incarnation(&request.disk, (&request.volume, &request.path), value, expected)
                .await;
            return Ok(Response::new(WriteMetadataResponse {
                success: result.is_ok(),
                error: result.err().map(Into::into),
            }));
        }
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            let write = match disk.begin_legacy_metadata_write(&request.volume, &request.path).await {
                Ok(write) => write,
                Err(err) => {
                    return Ok(Response::new(WriteMetadataResponse {
                        success: false,
                        error: Some(err.into()),
                    }));
                }
            };
            target.reject_legacy_artifact_mutation(&request.volume).await?;
            let file_info = match decode_msgpack_or_json::<FileInfo>(&request.file_info_bin, &request.file_info, "FileInfo") {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(WriteMetadataResponse {
                        success: false,
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            match write.write_metadata(file_info).await {
                Ok(_) => Ok(Response::new(WriteMetadataResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(WriteMetadataResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(WriteMetadataResponse {
                success: false,
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_update_metadata(
        &self,
        request: Request<UpdateMetadataRequest>,
    ) -> Result<Response<UpdateMetadataResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_update_metadata_request_body(request.get_ref()),
            "update_metadata",
        )?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            let opts = match decode_msgpack_or_json::<UpdateMetadataOpts>(&request.opts_bin, &request.opts, "UpdateMetadataOpts")
            {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(Response::new(UpdateMetadataResponse {
                        success: false,
                        error: Some(DiskError::other(format!("decode UpdateMetadataOpts failed: {err}")).into()),
                    }));
                }
            };

            let prepared = if opts.transition_reconcile.is_none() {
                match disk.begin_legacy_metadata_write(&request.volume, &request.path).await {
                    Ok(write) => Some(write),
                    Err(err) => {
                        return Ok(Response::new(UpdateMetadataResponse {
                            success: false,
                            error: Some(err.into()),
                        }));
                    }
                }
            } else {
                None
            };
            target.reject_legacy_artifact_mutation(&request.volume).await?;
            let file_info = match decode_msgpack_or_json::<FileInfo>(&request.file_info_bin, &request.file_info, "FileInfo") {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(UpdateMetadataResponse {
                        success: false,
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };

            let result = if let Some(write) = prepared {
                write.update_metadata(file_info, &opts).await
            } else {
                disk.update_metadata(&request.volume, &request.path, file_info, &opts).await
            };
            match result {
                Ok(_) => Ok(Response::new(UpdateMetadataResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(UpdateMetadataResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(UpdateMetadataResponse {
                success: false,
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_read_metadata(
        &self,
        request: Request<ReadMetadataRequest>,
    ) -> Result<Response<ReadMetadataResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_metadata(&request.volume, &request.path).await {
                Ok(data) => Ok(Response::new(ReadMetadataResponse {
                    success: true,
                    data,
                    error: None,
                })),
                Err(err) => Ok(Response::new(ReadMetadataResponse {
                    success: false,
                    data: Bytes::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadMetadataResponse {
                success: false,
                data: Bytes::new(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_delete_paths(
        &self,
        request: Request<DeletePathsRequest>,
    ) -> Result<Response<DeletePathsResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_delete_paths_request_body(request.get_ref()),
            "delete_paths",
        )?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        let result = match target
            .begin_legacy_local_path_delete(&request.disk, &request.volume, LegacyPathDeleteOperation::DeletePaths, None)
            .await
        {
            Ok(delete) => {
                target.reject_legacy_artifact_mutation(&request.volume).await?;
                delete.delete_paths(&request.paths).await
            }
            Err(err) => Err(err),
        };
        Ok(Response::new(DeletePathsResponse {
            success: result.is_ok(),
            error: result.err().map(Into::into),
        }))
    }

    pub(super) async fn handle_stat_volume(
        &self,
        request: Request<StatVolumeRequest>,
    ) -> Result<Response<StatVolumeResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.stat_volume(&request.volume).await {
                Ok(volume_info) => match serde_json::to_string(&volume_info) {
                    Ok(volume_info) => Ok(Response::new(StatVolumeResponse {
                        success: true,
                        volume_info,
                        error: None,
                    })),
                    Err(err) => Ok(Response::new(StatVolumeResponse {
                        success: false,
                        volume_info: String::new(),
                        error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                    })),
                },
                Err(err) => Ok(Response::new(StatVolumeResponse {
                    success: false,
                    volume_info: String::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(StatVolumeResponse {
                success: false,
                volume_info: String::new(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_list_volumes(
        &self,
        request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.list_volumes().await {
                Ok(volume_infos) => {
                    let volume_infos = volume_infos
                        .into_iter()
                        .enumerate()
                        .map(|(index, volume_info)| {
                            serde_json::to_string(&volume_info)
                                .map_err(|err| DiskError::other(format!("encode list volumes entry {index} failed: {err}")))
                        })
                        .collect::<std::result::Result<Vec<_>, DiskError>>();
                    match volume_infos {
                        Ok(volume_infos) => Ok(Response::new(ListVolumesResponse {
                            success: true,
                            volume_infos,
                            error: None,
                        })),
                        Err(err) => Ok(Response::new(ListVolumesResponse {
                            success: false,
                            volume_infos: Vec::new(),
                            error: Some(err.into()),
                        })),
                    }
                }
                Err(err) => Ok(Response::new(ListVolumesResponse {
                    success: false,
                    volume_infos: Vec::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ListVolumesResponse {
                success: false,
                volume_infos: Vec::new(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_make_volume(
        &self,
        request: Request<MakeVolumeRequest>,
    ) -> Result<Response<MakeVolumeResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_make_volume_request_body(request.get_ref()),
            "make_volume",
        )?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            match disk.make_volume(&request.volume).await {
                Ok(_) => Ok(Response::new(MakeVolumeResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(MakeVolumeResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(MakeVolumeResponse {
                success: false,
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_make_volumes(
        &self,
        request: Request<MakeVolumesRequest>,
    ) -> Result<Response<MakeVolumesResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_make_volumes_request_body(request.get_ref()),
            "make_volumes",
        )?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            match disk.make_volumes(request.volumes.iter().map(|s| &**s).collect()).await {
                Ok(_) => Ok(Response::new(MakeVolumesResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(MakeVolumesResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(MakeVolumesResponse {
                success: false,
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_rename_data(
        &self,
        request: Request<RenameDataRequest>,
        quota_fence: bool,
    ) -> Result<Response<RenameDataResponse>, Status> {
        let service_return_started = rustfs_io_metrics::put_stage_timer();
        let service_entry_started = rustfs_io_metrics::put_stage_timer();
        if !request.get_ref().scanner_publication_lease_token.is_empty() || !request.get_ref().bucket_incarnation_id.is_empty() {
            let has_body_digest = request
                .metadata()
                .get("x-rustfs-content-sha256")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value != "UNSIGNED-PAYLOAD");
            if !has_body_digest {
                rustfs_io_metrics::record_put_object_stage_duration_from(
                    rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_ENTRY,
                    service_entry_started,
                );
                rustfs_io_metrics::record_put_object_stage_duration_from(
                    rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_RESPONSE_RETURN,
                    service_return_started,
                );
                return Err(Status::permission_denied("scanner publication lease rename requires a body-bound digest"));
            }
        }
        let body = rustfs_protos::canonical_rename_data_request_body(request.get_ref());
        let mut quota_topology = None;
        let authenticated = if quota_fence {
            verify_quota_mutation_digest(&request, body).map(|topology| quota_topology = Some(topology))
        } else {
            verify_disk_mutation_digest(&request, body, "rename_data")
        };
        if let Err(err) = authenticated {
            rustfs_io_metrics::record_put_object_stage_duration_from(
                rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_ENTRY,
                service_entry_started,
            );
            rustfs_io_metrics::record_put_object_stage_duration_from(
                rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_RESPONSE_RETURN,
                service_return_started,
            );
            return Err(err);
        }
        rustfs_io_metrics::record_put_object_stage_duration_from(
            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_ENTRY,
            service_entry_started,
        );
        let before_handler_started = rustfs_io_metrics::put_stage_timer();
        let request = request.into_inner();
        let target = self.local_mutation_target();
        let bucket_incarnation = if request.bucket_incarnation_id.is_empty() {
            None
        } else {
            let expected = Uuid::from_slice(&request.bucket_incarnation_id)
                .ok()
                .filter(|id| !id.is_nil())
                .ok_or_else(|| Status::invalid_argument("bucket incarnation must be a non-nil UUID"))?;
            if !request.scanner_publication_lease_token.is_empty() {
                rustfs_io_metrics::record_put_object_stage_duration_from(
                    rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_BEFORE_HANDLER,
                    before_handler_started,
                );
                rustfs_io_metrics::record_put_object_stage_duration_from(
                    rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_RESPONSE_RETURN,
                    service_return_started,
                );
                return Err(Status::invalid_argument(
                    "heal incarnation and scanner publication lease cannot be combined",
                ));
            }
            Some(expected)
        };
        #[cfg(feature = "e2e-test-hooks")]
        super::rename_target_capture_test_hook::wait(&target, &request).await;
        let decoded_file_info = match decode_rename_data_request_file_info(&request.file_info_bin, &request.file_info) {
            Ok(file_info) => file_info,
            Err(err) => {
                rustfs_io_metrics::record_put_object_stage_duration_from(
                    rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_BEFORE_HANDLER,
                    before_handler_started,
                );
                let response_build_started = rustfs_io_metrics::put_stage_timer();
                let response = Response::new(RenameDataResponse {
                    success: false,
                    rename_data_resp: String::new(),
                    rename_data_resp_bin: Vec::new().into(),
                    error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                });
                rustfs_io_metrics::record_put_object_stage_duration_from(
                    rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_RESPONSE_BUILD,
                    response_build_started,
                );
                rustfs_io_metrics::record_put_object_stage_duration_from(
                    rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_RESPONSE_RETURN,
                    service_return_started,
                );
                return Ok(response);
            }
        };
        let scanner_publication_lease_token = if request.scanner_publication_lease_token.is_empty() {
            None
        } else {
            let token = Uuid::from_slice(&request.scanner_publication_lease_token)
                .map_err(|_| Status::invalid_argument("scanner publication lease token must be a UUID"))?;
            if token.is_nil() {
                rustfs_io_metrics::record_put_object_stage_duration_from(
                    rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_BEFORE_HANDLER,
                    before_handler_started,
                );
                rustfs_io_metrics::record_put_object_stage_duration_from(
                    rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_RESPONSE_RETURN,
                    service_return_started,
                );
                return Err(Status::invalid_argument("scanner publication lease token must not be nil"));
            }
            Some(token)
        };
        if quota_fence {
            let token = rustfs_utils::http::metadata_compat::get_consistent_str(
                &decoded_file_info.value.metadata,
                QUOTA_MUTATION_FENCE_METADATA_SUFFIX,
            )
            .and_then(|value| Uuid::parse_str(value).ok())
            .filter(|token| !token.is_nil());
            if token.is_none() {
                return Err(Status::invalid_argument(
                    "quota rename requires a consistent non-nil mutation fence token",
                ));
            }
        }
        if let Some(topology) = quota_topology {
            self.quota_store_for_topology(&target, &topology)?;
        }
        let request_decoded_from_msgpack = decoded_file_info.from_msgpack;
        // Bind ordinary mutations to the captured instance and both live volumes
        // before consulting bucket authority; commit consumes those exact locks.
        let prepared = if scanner_publication_lease_token.is_none() && bucket_incarnation.is_none() {
            match target
                .begin_legacy_local_rename_data(
                    &request.disk,
                    (&request.src_volume, &request.src_path),
                    &decoded_file_info.value,
                    (&request.dst_volume, &request.dst_path),
                )
                .await
            {
                Ok(rename) => Some(rename),
                Err(err) => {
                    return Ok(Response::new(RenameDataResponse {
                        success: false,
                        rename_data_resp: String::new(),
                        rename_data_resp_bin: Vec::new().into(),
                        error: Some(err.into()),
                    }));
                }
            }
        } else {
            None
        };
        target.reject_legacy_artifact_mutation(&request.src_volume).await?;
        target.reject_legacy_artifact_mutation(&request.dst_volume).await?;
        #[cfg(feature = "e2e-test-hooks")]
        let observation = startup_cas_rename_observation(&target, &request, &decoded_file_info.value);
        rustfs_io_metrics::record_put_object_stage_duration_from(
            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_BEFORE_HANDLER,
            before_handler_started,
        );
        let handler_await_started = rustfs_io_metrics::put_stage_timer();
        let result = if let Some(rename) = prepared {
            rename.commit().await
        } else {
            target
                .rename_local_data(
                    &request.disk,
                    (&request.src_volume, &request.src_path),
                    &decoded_file_info.value,
                    (&request.dst_volume, &request.dst_path),
                    scanner_publication_lease_token,
                    bucket_incarnation,
                )
                .await
        };
        rustfs_io_metrics::record_put_object_stage_duration_from(
            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_HANDLER_AWAIT,
            handler_await_started,
        );
        let after_handler_started = rustfs_io_metrics::put_stage_timer();
        #[cfg(feature = "e2e-test-hooks")]
        if let Some(mut observation) = observation {
            observation["ok"] = serde_json::json!(result.is_ok());
            observation["error"] = serde_json::json!(result.as_ref().err().map(ToString::to_string));
            let line = format!("RUSTFS_E2E_STARTUP_CAS {observation}\n");
            let _ = std::io::Write::write_all(&mut std::io::stderr().lock(), line.as_bytes());
        }
        rustfs_io_metrics::record_put_object_stage_duration_from(
            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_AFTER_HANDLER,
            after_handler_started,
        );
        let response_build_started = rustfs_io_metrics::put_stage_timer();
        let response_encode_started = rustfs_io_metrics::put_stage_timer();
        let response = match result {
            Ok(rename_data_resp) => match encode_rename_data_response_payloads(&rename_data_resp, request_decoded_from_msgpack) {
                Ok((rename_data_resp, rename_data_resp_bin)) => Response::new(RenameDataResponse {
                    success: true,
                    rename_data_resp,
                    rename_data_resp_bin: rename_data_resp_bin.into(),
                    error: None,
                }),
                Err(err) => Response::new(RenameDataResponse {
                    success: false,
                    rename_data_resp: String::new(),
                    rename_data_resp_bin: Vec::new().into(),
                    error: Some(err.into()),
                }),
            },
            Err(err) => Response::new(RenameDataResponse {
                success: false,
                rename_data_resp: String::new(),
                rename_data_resp_bin: Vec::new().into(),
                error: Some(err.into()),
            }),
        };
        rustfs_io_metrics::record_put_object_stage_duration_from(
            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_RESPONSE_ENCODE,
            response_encode_started,
        );
        rustfs_io_metrics::record_put_object_stage_duration_from(
            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_RESPONSE_BUILD,
            response_build_started,
        );
        rustfs_io_metrics::record_put_object_stage_duration_from(
            rustfs_io_metrics::PUT_STAGE_SET_DISK_RENAME_REMOTE_SERVICE_RESPONSE_RETURN,
            service_return_started,
        );
        Ok(response)
    }

    pub(super) async fn handle_list_dir(&self, request: Request<ListDirRequest>) -> Result<Response<ListDirResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.list_dir("", &request.volume, &request.dir_path, request.count).await {
                Ok(volumes) => Ok(Response::new(ListDirResponse {
                    success: true,
                    volumes,
                    error: None,
                })),
                Err(err) => Ok(Response::new(ListDirResponse {
                    success: false,
                    volumes: Vec::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ListDirResponse {
                success: false,
                volumes: Vec::new(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_write(&self, _request: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        unimplemented!("write");
    }

    pub(super) async fn handle_rename_file(
        &self,
        request: Request<RenameFileRequest>,
    ) -> Result<Response<RenameFileResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_rename_file_request_body(request.get_ref()),
            "rename_file",
        )?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        if target.find_mutation_disk(&request.disk).await.is_some() {
            let result = match target
                .begin_legacy_local_rename_file(
                    &request.disk,
                    (&request.src_volume, &request.src_path),
                    (&request.dst_volume, &request.dst_path),
                    request.durable,
                )
                .await
            {
                Ok(rename) => {
                    target.reject_legacy_artifact_mutation(&request.src_volume).await?;
                    target.reject_legacy_artifact_mutation(&request.dst_volume).await?;
                    rename.commit().await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(_) => Ok(Response::new(RenameFileResponse {
                    success: true,
                    durability_applied: request.durable,
                    error: None,
                })),
                Err(err) => Ok(Response::new(RenameFileResponse {
                    success: false,
                    durability_applied: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(RenameFileResponse {
                success: false,
                durability_applied: false,
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_rename_part(
        &self,
        request: Request<RenamePartRequest>,
    ) -> Result<Response<RenamePartResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_rename_part_request_body(request.get_ref()),
            "rename_part",
        )?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        if target.find_mutation_disk(&request.disk).await.is_some() {
            let result = match target
                .begin_legacy_local_part_mutation(
                    &request.disk,
                    Some((&request.src_volume, &request.src_path)),
                    (&request.dst_volume, &request.dst_path),
                    LegacyPartOperation::Rename,
                )
                .await
            {
                Ok(part) => {
                    target.reject_legacy_artifact_mutation(&request.src_volume).await?;
                    target.reject_legacy_artifact_mutation(&request.dst_volume).await?;
                    part.rename(request.meta).await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(_) => Ok(Response::new(RenamePartResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(RenamePartResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(RenamePartResponse {
                success: false,
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_prepare_part_transaction(
        &self,
        request: Request<PreparePartTransactionRequest>,
    ) -> Result<Response<PreparePartTransactionResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_prepare_part_transaction_request_body(request.get_ref()),
            "prepare_part_transaction",
        )?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        if target.find_mutation_disk(&request.disk).await.is_some() {
            let result = match target
                .begin_legacy_local_part_mutation(
                    &request.disk,
                    Some((&request.src_volume, &request.src_path)),
                    (&request.dst_volume, &request.dst_path),
                    LegacyPartOperation::Prepare,
                )
                .await
            {
                Ok(part) => {
                    target.reject_legacy_artifact_mutation(&request.src_volume).await?;
                    target.reject_legacy_artifact_mutation(&request.dst_volume).await?;
                    part.prepare(request.meta).await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => Ok(Response::new(PreparePartTransactionResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(PreparePartTransactionResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(PreparePartTransactionResponse {
                success: false,
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_recover_delete_transaction(
        &self,
        request: Request<RecoverDeleteTransactionRequest>,
    ) -> Result<Response<RecoverDeleteTransactionResponse>, Status> {
        if request.get_ref().operation.len() > rustfs_protos::DELETE_RECOVERY_MAX_PAYLOAD_BYTES {
            return Err(Status::resource_exhausted("delete recovery operation exceeds payload limit"));
        }
        let body = rustfs_protos::canonical_recover_delete_transaction_request_body(request.get_ref())
            .map_err(|_| Status::invalid_argument("delete recovery request length cannot be represented"))?;
        // This additive protocol has no digestless rolling-upgrade exception.
        verify_tonic_canonical_body_digest(&request, &body)
            .map_err(|_| Status::permission_denied("delete recovery requires body-bound v2 authentication"))?;
        let request = request.into_inner();
        if request.protocol_version != DELETE_RECOVERY_PROTOCOL_VERSION {
            return Err(Status::unimplemented("incompatible delete recovery protocol"));
        }
        let operation: DeleteRecoveryRpcOperation = rustfs_protos::decode_delete_recovery_payload(&request.operation)
            .map_err(|_| Status::invalid_argument("invalid delete recovery operation"))?;
        if request.operation.len() > operation.max_wire_payload_bytes() {
            return Err(Status::resource_exhausted("delete recovery operation exceeds payload limit"));
        }
        match &operation {
            DeleteRecoveryRpcOperation::ApplyVersions { versions, .. }
                if versions.len() > 1000
                    || versions
                        .iter()
                        .try_fold(0_usize, |count, versions| count.checked_add(versions.versions.len()))
                        .is_none_or(|count| count > 1000) =>
            {
                return Err(Status::invalid_argument("recoverable batch delete exceeds 1000 object/version requests"));
            }
            DeleteRecoveryRpcOperation::Discover { limit, .. } if *limit == 0 || *limit > 256 => {
                return Err(Status::invalid_argument("delete recovery page limit must be between 1 and 256"));
            }
            DeleteRecoveryRpcOperation::Inspect { transaction, .. } | DeleteRecoveryRpcOperation::Settle { transaction, .. }
                if transaction.is_nil() =>
            {
                return Err(Status::invalid_argument("delete transaction must be a non-nil UUID"));
            }
            DeleteRecoveryRpcOperation::ApplyVersion { opts, .. } | DeleteRecoveryRpcOperation::ApplyVersions { opts, .. }
                if opts.old_data_dir.is_none_or(|transaction| transaction.is_nil())
                    || opts.undo_delete
                    || opts.undo_write
                    || opts.recursive
                    || opts.immediate
                    || opts.orphan_data_dir
                    || opts.expected_delete_marker.is_some()
                    || opts
                        .delete_recovery_bucket_incarnation
                        .is_some_and(|incarnation| incarnation.is_nil()) =>
            {
                return Err(Status::invalid_argument(
                    "recoverable delete requires a non-nil transaction and valid apply options",
                ));
            }
            DeleteRecoveryRpcOperation::ApplyVersion { object, .. } if object.starts_with('/') => {
                return Err(Status::invalid_argument("recoverable delete requires a relative object key"));
            }
            DeleteRecoveryRpcOperation::ApplyVersions { versions, .. }
                if versions.iter().any(|version| version.name.starts_with('/')) =>
            {
                return Err(Status::invalid_argument("recoverable delete requires relative object keys"));
            }
            _ => {}
        }
        let target = self.local_mutation_target();
        if matches!(
            operation,
            DeleteRecoveryRpcOperation::ApplyVersion { .. }
                | DeleteRecoveryRpcOperation::ApplyVersions { .. }
                | DeleteRecoveryRpcOperation::Settle { .. }
        ) {
            target.reject_legacy_artifact_mutation(&request.volume).await?;
        }
        let result = target
            .recover_local_delete_transaction(&request.disk, &request.volume, operation)
            .await;
        match result {
            Ok(reply) => {
                let result = encode_msgpack_named(&reply, "delete recovery reply")
                    .map_err(|_| Status::internal("cannot encode delete recovery reply"))?;
                if result.len() > rustfs_protos::DELETE_RECOVERY_MAX_PAYLOAD_BYTES {
                    return Err(Status::resource_exhausted("delete recovery reply exceeds payload limit"));
                }
                Ok(Response::new(RecoverDeleteTransactionResponse {
                    success: true,
                    protocol_version: DELETE_RECOVERY_PROTOCOL_VERSION,
                    result: result.into(),
                    error: None,
                }))
            }
            Err(error) => Ok(Response::new(RecoverDeleteTransactionResponse {
                success: false,
                protocol_version: DELETE_RECOVERY_PROTOCOL_VERSION,
                result: Bytes::new(),
                error: Some(error.into()),
            })),
        }
    }

    pub(super) async fn handle_settle_part_transaction(
        &self,
        request: Request<SettlePartTransactionRequest>,
    ) -> Result<Response<SettlePartTransactionResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_settle_part_transaction_request_body(request.get_ref()),
            "settle_part_transaction",
        )?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        if target.find_mutation_disk(&request.disk).await.is_some() {
            let result = match target
                .begin_legacy_local_part_mutation(
                    &request.disk,
                    None,
                    (&request.volume, &request.path),
                    LegacyPartOperation::Settle,
                )
                .await
            {
                Ok(part) => {
                    target.reject_legacy_artifact_mutation(&request.volume).await?;
                    let action = if request.rollback {
                        PartTransactionAction::Rollback
                    } else {
                        PartTransactionAction::Commit
                    };
                    part.settle(action).await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => Ok(Response::new(SettlePartTransactionResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(SettlePartTransactionResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(SettlePartTransactionResponse {
                success: false,
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_check_parts(
        &self,
        request: Request<CheckPartsRequest>,
    ) -> Result<Response<CheckPartsResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(CheckPartsResponse {
                        success: false,
                        check_parts_resp: "".to_string(),
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            match disk.check_parts(&request.volume, &request.path, &file_info).await {
                Ok(check_parts_resp) => {
                    let check_parts_resp = match serde_json::to_string(&check_parts_resp) {
                        Ok(check_parts_resp) => check_parts_resp,
                        Err(err) => {
                            return Ok(Response::new(CheckPartsResponse {
                                success: false,
                                check_parts_resp: String::new(),
                                error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                            }));
                        }
                    };
                    Ok(Response::new(CheckPartsResponse {
                        success: true,
                        check_parts_resp,
                        error: None,
                    }))
                }
                Err(err) => Ok(Response::new(CheckPartsResponse {
                    success: false,
                    check_parts_resp: "".to_string(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(CheckPartsResponse {
                success: false,
                check_parts_resp: "".to_string(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_read_parts(
        &self,
        request: Request<ReadPartsRequest>,
    ) -> Result<Response<ReadPartsResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_parts(&request.bucket, &request.paths).await {
                Ok(data) => {
                    let data = match rmp_serde::to_vec(&data) {
                        Ok(data) => data,
                        Err(err) => {
                            return Ok(Response::new(ReadPartsResponse {
                                success: false,
                                object_part_infos: Bytes::new(),
                                error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                            }));
                        }
                    };
                    Ok(Response::new(ReadPartsResponse {
                        success: true,
                        object_part_infos: Bytes::copy_from_slice(&data),
                        error: None,
                    }))
                }

                Err(err) => Ok(Response::new(ReadPartsResponse {
                    success: false,
                    object_part_infos: Bytes::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadPartsResponse {
                success: false,
                object_part_infos: Bytes::new(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_verify_file(
        &self,
        request: Request<VerifyFileRequest>,
    ) -> Result<Response<VerifyFileResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(VerifyFileResponse {
                        success: false,
                        check_parts_resp: "".to_string(),
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            match disk.verify_file(&request.volume, &request.path, &file_info).await {
                Ok(check_parts_resp) => {
                    let check_parts_resp = match serde_json::to_string(&check_parts_resp) {
                        Ok(check_parts_resp) => check_parts_resp,
                        Err(err) => {
                            return Ok(Response::new(VerifyFileResponse {
                                success: false,
                                check_parts_resp: String::new(),
                                error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                            }));
                        }
                    };
                    Ok(Response::new(VerifyFileResponse {
                        success: true,
                        check_parts_resp,
                        error: None,
                    }))
                }
                Err(err) => Ok(Response::new(VerifyFileResponse {
                    success: false,
                    check_parts_resp: "".to_string(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(VerifyFileResponse {
                success: false,
                check_parts_resp: "".to_string(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        if !request.get_ref().scanner_publication_lease_token.is_empty() {
            let has_body_digest = request
                .metadata()
                .get("x-rustfs-content-sha256")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value != "UNSIGNED-PAYLOAD");
            if !has_body_digest {
                return Err(Status::permission_denied("scanner publication lease delete requires a body-bound digest"));
            }
        }
        verify_disk_mutation_digest(&request, rustfs_protos::canonical_delete_request_body(request.get_ref()), "delete")?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        if !request.bucket_incarnation_id.is_empty() {
            target.reject_legacy_artifact_mutation(&request.volume).await?;
            if !request.scanner_publication_lease_token.is_empty() {
                return Err(Status::invalid_argument("heal incarnation and scanner lease cannot be combined"));
            }
            let expected = Uuid::from_slice(&request.bucket_incarnation_id)
                .ok()
                .filter(|id| !id.is_nil())
                .ok_or_else(|| Status::invalid_argument("bucket incarnation must be a non-nil UUID"))?;
            let LocalMutationTarget::Ready(store) = target else {
                return Err(Status::failed_precondition("bucket heal requires a ready storage instance"));
            };
            let value = serde_json::from_str::<DeleteOptions>(&request.options)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let result = store
                .delete_local_path_at_incarnation(&request.disk, (&request.volume, &request.path), value, expected)
                .await;
            return Ok(Response::new(DeleteResponse {
                success: result.is_ok(),
                error: result.err().map(Into::into),
            }));
        }
        let options = match serde_json::from_str::<DeleteOptions>(&request.options) {
            Ok(options) => options,
            Err(err) => {
                return Ok(Response::new(DeleteResponse {
                    success: false,
                    error: Some(DiskError::other(format!("decode DeleteOptions failed: {err}")).into()),
                }));
            }
        };
        let scanner_publication_lease_token = if request.scanner_publication_lease_token.is_empty() {
            None
        } else {
            let token = Uuid::from_slice(&request.scanner_publication_lease_token)
                .map_err(|_| Status::invalid_argument("scanner publication lease token must be a UUID"))?;
            if token.is_nil() {
                return Err(Status::invalid_argument("scanner publication lease token must not be nil"));
            }
            Some(token)
        };
        // Resolve the guard from the captured receiver and retain it in the
        // physical delete owner, including after caller cancellation.
        let scanner_publication_lease_guard: Option<Arc<dyn Send + Sync>> = if let Some(token) = scanner_publication_lease_token {
            let LocalMutationTarget::Ready(store) = &target else {
                return Ok(Response::new(DeleteResponse {
                    success: false,
                    error: Some(DiskError::other("scanner publication lease owner is unavailable").into()),
                }));
            };
            match store.acquire_scanner_publication_lease_guard(token).await {
                Ok(guard) => Some(Arc::new(guard)),
                Err(err) => {
                    return Ok(Response::new(DeleteResponse {
                        success: false,
                        error: Some(DiskError::other(err.to_string()).into()),
                    }));
                }
            }
        } else {
            None
        };
        let result = match target
            .begin_legacy_local_path_delete(
                &request.disk,
                &request.volume,
                LegacyPathDeleteOperation::Delete,
                scanner_publication_lease_guard,
            )
            .await
        {
            Ok(delete) => {
                target.reject_legacy_artifact_mutation(&request.volume).await?;
                delete.delete(&request.path, options).await
            }
            Err(err) => Err(err),
        };
        Ok(Response::new(DeleteResponse {
            success: result.is_ok(),
            error: result.err().map(Into::into),
        }))
    }

    pub(super) async fn handle_write_all(&self, request: Request<WriteAllRequest>) -> Result<Response<WriteAllResponse>, Status> {
        verify_disk_mutation_digest(&request, rustfs_protos::canonical_write_all_request_body(request.get_ref()), "write_all")?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        let data_len = request.data.len();
        let metrics = runtime_sources::current_internode_metrics();
        metrics.record_incoming_request_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_WRITE_ALL,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
        );
        metrics.record_recv_bytes_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_WRITE_ALL,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
            data_len,
        );
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            let result = match disk.begin_legacy_file_write(&request.volume).await {
                Ok(write) => {
                    target.reject_legacy_artifact_mutation(&request.volume).await?;
                    write.write_all(&request.path, request.data).await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(_) => Ok(Response::new(WriteAllResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => {
                    metrics.record_error_for_operation_and_backend(
                        INTERNODE_OPERATION_GRPC_WRITE_ALL,
                        INTERNODE_TRANSPORT_BACKEND_GRPC,
                    );
                    Ok(Response::new(WriteAllResponse {
                        success: false,
                        error: Some(err.into()),
                    }))
                }
            }
        } else {
            metrics.record_error_for_operation_and_backend(INTERNODE_OPERATION_GRPC_WRITE_ALL, INTERNODE_TRANSPORT_BACKEND_GRPC);
            Ok(Response::new(WriteAllResponse {
                success: false,
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_compare_and_update_file(
        &self,
        request: Request<CompareAndUpdateFileRequest>,
    ) -> Result<Response<CompareAndUpdateFileResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_compare_and_update_file_request_body(request.get_ref()),
            "compare_and_update_file",
        )?;
        let request = request.into_inner();
        let target = self.local_mutation_target();
        let data_len = request
            .expected
            .as_ref()
            .map_or(0, Bytes::len)
            .saturating_add(request.replacement.as_ref().map_or(0, Bytes::len));
        let metrics = runtime_sources::current_internode_metrics();
        metrics.record_incoming_request_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_COMPARE_AND_UPDATE_FILE,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
        );
        metrics.record_recv_bytes_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_COMPARE_AND_UPDATE_FILE,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
            data_len,
        );
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            let result = match disk.begin_legacy_file_write(&request.volume).await {
                Ok(write) => {
                    target.reject_legacy_artifact_mutation(&request.volume).await?;
                    write
                        .compare_and_update_file(&request.path, request.expected, request.replacement)
                        .await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(outcome) => Ok(Response::new(CompareAndUpdateFileResponse {
                    success: true,
                    outcome: match outcome {
                        ConditionalFileUpdate::Updated => CompareAndUpdateFileOutcome::CompareAndUpdateFileUpdated as i32,
                        ConditionalFileUpdate::Missing => CompareAndUpdateFileOutcome::CompareAndUpdateFileMissing as i32,
                        ConditionalFileUpdate::Mismatch => CompareAndUpdateFileOutcome::CompareAndUpdateFileMismatch as i32,
                    },
                    error: None,
                })),
                Err(err) => {
                    metrics.record_error_for_operation_and_backend(
                        INTERNODE_OPERATION_GRPC_COMPARE_AND_UPDATE_FILE,
                        INTERNODE_TRANSPORT_BACKEND_GRPC,
                    );
                    Ok(Response::new(CompareAndUpdateFileResponse {
                        success: false,
                        outcome: CompareAndUpdateFileOutcome::CompareAndUpdateFileUnspecified as i32,
                        error: Some(err.into()),
                    }))
                }
            }
        } else {
            metrics.record_error_for_operation_and_backend(
                INTERNODE_OPERATION_GRPC_COMPARE_AND_UPDATE_FILE,
                INTERNODE_TRANSPORT_BACKEND_GRPC,
            );
            Ok(Response::new(CompareAndUpdateFileResponse {
                success: false,
                outcome: CompareAndUpdateFileOutcome::CompareAndUpdateFileUnspecified as i32,
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_read_all(&self, request: Request<ReadAllRequest>) -> Result<Response<ReadAllResponse>, Status> {
        debug!("read all");

        let request = request.into_inner();
        let metrics = runtime_sources::current_internode_metrics();
        metrics.record_incoming_request_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_READ_ALL,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
        );
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_all(&request.volume, &request.path).await {
                Ok(data) => {
                    metrics.record_sent_bytes_for_operation_and_backend(
                        INTERNODE_OPERATION_GRPC_READ_ALL,
                        INTERNODE_TRANSPORT_BACKEND_GRPC,
                        data.len(),
                    );
                    Ok(Response::new(ReadAllResponse {
                        success: true,
                        data,
                        error: None,
                    }))
                }
                Err(err) => {
                    metrics.record_error_for_operation_and_backend(
                        INTERNODE_OPERATION_GRPC_READ_ALL,
                        INTERNODE_TRANSPORT_BACKEND_GRPC,
                    );
                    Ok(Response::new(ReadAllResponse {
                        success: false,
                        data: Bytes::new(),
                        error: Some(err.into()),
                    }))
                }
            }
        } else {
            metrics.record_error_for_operation_and_backend(INTERNODE_OPERATION_GRPC_READ_ALL, INTERNODE_TRANSPORT_BACKEND_GRPC);
            Ok(Response::new(ReadAllResponse {
                success: false,
                data: Bytes::new(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DeleteOptions, compat_response_json, decode_msgpack_or_json, decode_rename_data_request_file_info,
        encode_batch_read_version_response_payloads, encode_delete_versions_errors, encode_file_info_msgpack, encode_msgpack,
        encode_msgpack_named, encode_read_multiple_response_payloads, encode_rename_data_response_payloads,
        rejects_legacy_delete_transaction,
    };
    use crate::storage::DiskError;
    use crate::storage::rpc::node_service::make_server;
    use crate::storage::storage_api::ReadMultipleResp;
    use crate::storage::storage_api::RenameDataResp;
    use crate::storage::storage_api::rpc_consumer::node_service::BatchReadVersionResp;
    use rustfs_filemeta::FileInfo;
    use rustfs_io_metrics::internode_metrics::global_internode_metrics;
    use rustfs_protos::proto_gen::node_service::ReadVersionRequest;
    use serde::{Deserialize, Serialize};
    use serial_test::serial;
    use tonic::Request;
    use uuid::Uuid;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct SamplePayload {
        name: String,
        count: u32,
    }

    #[test]
    fn legacy_delete_transaction_policy_preserves_rollback_and_internal_maintenance() {
        let mut opts = DeleteOptions {
            old_data_dir: Some(Uuid::new_v4()),
            ..Default::default()
        };
        assert!(rejects_legacy_delete_transaction("bucket", &opts));
        assert!(!rejects_legacy_delete_transaction(".rustfs.sys", &opts));
        assert!(!rejects_legacy_delete_transaction(".rustfs.sys/tmp", &opts));
        assert!(!rejects_legacy_delete_transaction(".minio.sys", &opts));
        opts.undo_write = true;
        assert!(!rejects_legacy_delete_transaction("bucket", &opts));
        opts.undo_write = false;
        opts.recoverable_delete = true;
        assert!(!rejects_legacy_delete_transaction("bucket", &opts));
        opts.recoverable_delete = false;
        opts.old_data_dir = None;
        assert!(!rejects_legacy_delete_transaction("bucket", &opts));
    }

    #[test]
    fn response_compat_send_sites_keep_manifest_json_encoders() {
        // Rolling-upgrade contract (rustfs-protos compat manifest): every
        // dual-write response field must keep producing its JSON side with the
        // exact encoder the manifest pins. The manifest itself is pinned
        // against node.proto by tests in rustfs-protos; this test keeps the
        // send-site assertion in the crate that owns the source file.
        let source = rustfs_protos::compat_manifest::production_source(include_str!("disk.rs"), "disk.rs");

        for send_site in rustfs_protos::compat_manifest::RESPONSE_COMPAT_SEND_SITES {
            assert!(
                source.contains(send_site.json_encoder),
                "{}.{} must keep its manifest encoder: {}",
                send_site.field.message,
                send_site.field.json_field,
                send_site.json_encoder
            );
        }
    }

    #[test]
    fn delete_versions_response_dual_writes_typed_item_errors() {
        let raw_not_found = super::DiskError::Io(std::io::Error::from(std::io::ErrorKind::NotFound));
        let (errors, item_errors) = encode_delete_versions_errors(vec![Some(raw_not_found), None]);

        assert!(errors[0].starts_with("io error "));
        assert!(errors[1].is_empty());
        assert_eq!(item_errors[0].code, super::DiskError::FileNotFound.to_u32());
        assert_eq!(item_errors[0].error_info, errors[0]);
        assert_eq!(item_errors[1].code, 0);
    }

    #[tokio::test]
    #[serial]
    async fn handle_read_version_records_attribution_for_missing_disk() {
        let metrics = global_internode_metrics();
        let previous_stage_metrics = rustfs_io_metrics::get_stage_metrics_enabled();
        metrics.reset_for_test();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);

        let response = make_server()
            .handle_read_version(Request::new(ReadVersionRequest {
                disk: "missing-disk".to_string(),
                volume: "bucket".to_string(),
                path: "object".to_string(),
                version_id: String::new(),
                opts: String::new(),
                opts_bin: Vec::new().into(),
            }))
            .await
            .expect("ReadVersion handler should return a response")
            .into_inner();

        rustfs_io_metrics::set_get_stage_metrics_enabled(previous_stage_metrics);
        let snapshot = metrics.snapshot();
        assert!(!response.success);
        assert_eq!(snapshot.incoming_requests_total, 1);
        assert_eq!(snapshot.errors_total, 1);
        assert!(snapshot.recv_bytes_total > 0);
        metrics.reset_for_test();
    }

    #[test]
    fn decode_msgpack_or_json_prefers_binary_payload() {
        let payload = SamplePayload {
            name: "rustfs".to_string(),
            count: 3,
        };

        let binary = encode_msgpack(&payload, "SamplePayload").unwrap();
        let before = global_internode_metrics().msgpack_json_decode_total_for_test();
        let decoded =
            decode_msgpack_or_json::<SamplePayload>(&binary, r#"{"name":"ignored","count":1}"#, "SamplePayload").unwrap();
        let after = global_internode_metrics().msgpack_json_decode_total_for_test();

        assert_eq!(decoded, payload);
        assert!(after > before, "successful request msgpack decode should increment traffic metrics");
    }

    #[test]
    fn decode_msgpack_or_json_falls_back_to_json() {
        let before = global_internode_metrics().msgpack_json_decode_total_for_test();
        let decoded = decode_msgpack_or_json::<SamplePayload>(&[], r#"{"name":"compat","count":7}"#, "SamplePayload").unwrap();
        let after = global_internode_metrics().msgpack_json_decode_total_for_test();

        assert_eq!(
            decoded,
            SamplePayload {
                name: "compat".to_string(),
                count: 7,
            }
        );
        assert!(after > before, "successful request JSON fallback decode should increment traffic metrics");
    }

    fn with_internode_msgpack_env<R>(vars: [(&'static str, Option<&'static str>); 2], f: impl FnOnce() -> R) -> R {
        temp_env::with_vars(vars, || {
            rustfs_protos::reset_internode_rpc_msgpack_only_cache();
            let result = f();
            rustfs_protos::reset_internode_rpc_msgpack_only_cache();
            result
        })
    }

    #[test]
    fn compat_response_json_keeps_json_when_msgpack_only_lacks_fleet_confirmation() {
        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("true")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, None::<&str>),
            ],
            || {
                let payload = SamplePayload {
                    name: "legacy-json".to_string(),
                    count: 9,
                };
                let json = compat_response_json(&payload, false).expect("compat response json should encode");

                assert!(!json.is_empty(), "old JSON clients must remain compatible without fleet confirmation");
                assert_eq!(json, serde_json::to_string(&payload).expect("json should encode"));
            },
        );
    }

    #[test]
    fn compat_response_json_omits_json_only_after_fleet_confirmation() {
        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("true")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, Some("true")),
            ],
            || {
                let payload = SamplePayload {
                    name: "msgpack-only".to_string(),
                    count: 10,
                };
                let json = compat_response_json(&payload, false).expect("compat response json should encode");

                assert!(json.is_empty(), "msgpack-only may empty JSON only after explicit fleet confirmation");
            },
        );
    }

    #[test]
    fn compat_response_json_restores_json_when_either_msgpack_only_gate_is_removed() {
        let payload = SamplePayload {
            name: "rollback".to_string(),
            count: 12,
        };

        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("true")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, Some("true")),
            ],
            || {
                let json = compat_response_json(&payload, false).expect("compat response json should encode");

                assert!(json.is_empty(), "both gates should enter msgpack-only response mode");
            },
        );

        for vars in [
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("true")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, Some("false")),
            ],
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("false")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, Some("true")),
            ],
        ] {
            with_internode_msgpack_env(vars, || {
                let json = compat_response_json(&payload, false).expect("compat response json should encode");

                assert!(!json.is_empty(), "removing either gate should restore old-peer JSON compatibility");
                assert_eq!(json, serde_json::to_string(&payload).expect("json should encode"));
            });
        }
    }

    #[test]
    fn compat_response_json_omits_json_for_msgpack_capable_request_without_fleet_gate() {
        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, None::<&str>),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, None::<&str>),
            ],
            || {
                let payload = SamplePayload {
                    name: "request-local-bin".to_string(),
                    count: 13,
                };
                let json = compat_response_json(&payload, true).expect("compat response json should encode");

                assert!(json.is_empty(), "a request-local msgpack payload may receive a bin-only response");
            },
        );
    }

    #[test]
    fn compat_response_json_keeps_json_for_legacy_json_request() {
        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, None::<&str>),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, None::<&str>),
            ],
            || {
                let payload = SamplePayload {
                    name: "legacy-request-json".to_string(),
                    count: 14,
                };
                let json = compat_response_json(&payload, false).expect("compat response json should encode");

                assert!(!json.is_empty(), "legacy JSON-only callers must keep receiving response JSON");
                assert_eq!(json, serde_json::to_string(&payload).expect("json should encode"));
            },
        );
    }

    #[test]
    fn rename_data_response_payloads_follow_successful_request_codec() {
        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, None::<&str>),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, None::<&str>),
            ],
            || {
                let response = RenameDataResp::default();
                let file_info = FileInfo::default();
                let legacy_file_info_json = serde_json::to_string(&file_info).expect("FileInfo JSON should encode");
                let legacy_request = decode_rename_data_request_file_info(&[], &legacy_file_info_json)
                    .expect("legacy FileInfo JSON should decode");

                let (legacy_json, legacy_bin) = encode_rename_data_response_payloads(&response, legacy_request.from_msgpack)
                    .expect("legacy response payloads should encode");
                assert!(!legacy_json.is_empty(), "JSON-only requests must retain response JSON");
                assert!(!legacy_bin.is_empty(), "all callers must receive response msgpack");

                let file_info_bin = encode_file_info_msgpack(&file_info).expect("FileInfo msgpack should encode");
                let msgpack_request = decode_rename_data_request_file_info(&file_info_bin, &legacy_file_info_json)
                    .expect("FileInfo msgpack should decode");
                let (msgpack_json, msgpack_bin) = encode_rename_data_response_payloads(&response, msgpack_request.from_msgpack)
                    .expect("msgpack response payloads should encode");
                assert!(msgpack_json.is_empty(), "successfully decoded msgpack requests may omit response JSON");
                let decoded: RenameDataResp =
                    rmp_serde::from_slice(&msgpack_bin).expect("response msgpack should remain decodable");
                assert_eq!(decoded.old_data_dir, response.old_data_dir);
                assert_eq!(decoded.rollback_data_dir, response.rollback_data_dir);
                assert_eq!(decoded.cleanup_data_dir, response.cleanup_data_dir);
                assert_eq!(decoded.sign, response.sign);
                assert_eq!(decoded.old_current_size, response.old_current_size);

                let error = match decode_rename_data_request_file_info(b"not-msgpack", &legacy_file_info_json) {
                    Ok(_) => panic!("malformed request msgpack must fail closed before response negotiation"),
                    Err(error) => error,
                };
                assert!(error.to_string().contains("decode FileInfo msgpack failed"), "unexpected error: {error}");
            },
        );
    }

    #[test]
    fn decode_msgpack_or_json_fails_closed_on_corrupt_non_empty_msgpack() {
        let before = global_internode_metrics().msgpack_json_decode_error_total_for_test();
        let err = decode_msgpack_or_json::<SamplePayload>(b"not-msgpack", r#"{"name":"json","count":1}"#, "SamplePayload")
            .expect_err("corrupt non-empty msgpack must not fall back to JSON");
        let after = global_internode_metrics().msgpack_json_decode_error_total_for_test();

        assert!(err.to_string().contains("decode SamplePayload msgpack failed"), "unexpected error: {err}");
        assert!(after > before, "corrupt msgpack should increment decode-error metrics");
    }

    #[test]
    fn decode_msgpack_or_json_reports_corrupt_json_item_when_msgpack_absent() {
        let before = global_internode_metrics().msgpack_json_decode_error_total_for_test();
        let err = decode_msgpack_or_json::<SamplePayload>(&[], "{not-json", "SamplePayload")
            .expect_err("corrupt json item should fail in fallback branch");
        let after = global_internode_metrics().msgpack_json_decode_error_total_for_test();

        assert!(err.to_string().contains("decode SamplePayload failed"), "unexpected error: {err}");
        assert!(after > before, "corrupt fallback JSON should increment decode-error metrics");
    }

    #[test]
    fn decode_msgpack_or_json_accepts_named_msgpack_and_legacy_json() {
        let payload = SamplePayload {
            name: "named".to_string(),
            count: 11,
        };
        let named_msgpack = encode_msgpack_named(&payload, "SamplePayload").expect("named msgpack should encode");
        let decoded_msgpack =
            decode_msgpack_or_json::<SamplePayload>(&named_msgpack, "", "SamplePayload").expect("named msgpack should decode");
        let decoded_json = decode_msgpack_or_json::<SamplePayload>(&[], r#"{"name":"legacy-json","count":12}"#, "SamplePayload")
            .expect("legacy json should decode");

        assert_eq!(decoded_msgpack, payload);
        assert_eq!(
            decoded_json,
            SamplePayload {
                name: "legacy-json".to_string(),
                count: 12
            }
        );
    }

    #[test]
    fn encode_read_multiple_response_payloads_keeps_json_and_msgpack_in_sync() {
        let responses = vec![
            ReadMultipleResp {
                bucket: "bucket".to_string(),
                prefix: "prefix".to_string(),
                file: "a".to_string(),
                exists: true,
                data: b"alpha".to_vec(),
                ..Default::default()
            },
            ReadMultipleResp {
                bucket: "bucket".to_string(),
                prefix: "prefix".to_string(),
                file: "b".to_string(),
                exists: true,
                data: b"beta".to_vec(),
                ..Default::default()
            },
        ];

        let (json_payloads, msgpack_payloads) =
            encode_read_multiple_response_payloads(&responses).expect("read multiple responses should encode");

        assert_eq!(json_payloads.len(), responses.len());
        assert_eq!(msgpack_payloads.len(), responses.len());

        let json_decoded: ReadMultipleResp =
            serde_json::from_str(&json_payloads[0]).expect("json read multiple response should decode");
        let msgpack_decoded = decode_msgpack_or_json::<ReadMultipleResp>(&msgpack_payloads[0], "", "ReadMultipleResp")
            .expect("msgpack read multiple response should decode");

        assert_eq!(json_decoded.file, responses[0].file);
        assert_eq!(msgpack_decoded.file, responses[0].file);
        assert_eq!(msgpack_decoded.data, responses[0].data);
    }

    #[test]
    fn encode_read_multiple_response_payloads_respects_msgpack_only_gate_and_rollback() {
        let responses = vec![ReadMultipleResp {
            bucket: "bucket".to_string(),
            prefix: "prefix".to_string(),
            file: "gate".to_string(),
            exists: true,
            data: b"payload".to_vec(),
            ..Default::default()
        }];

        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("true")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, Some("true")),
            ],
            || {
                let (json_payloads, msgpack_payloads) =
                    encode_read_multiple_response_payloads(&responses).expect("read multiple responses should encode");

                assert_eq!(json_payloads, vec![String::new()]);
                assert_eq!(msgpack_payloads.len(), responses.len());
                let decoded = decode_msgpack_or_json::<ReadMultipleResp>(&msgpack_payloads[0], "", "ReadMultipleResp")
                    .expect("msgpack read multiple response should decode");
                assert_eq!(decoded.file, responses[0].file);
            },
        );

        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("true")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, Some("false")),
            ],
            || {
                let (json_payloads, msgpack_payloads) =
                    encode_read_multiple_response_payloads(&responses).expect("read multiple responses should encode");

                assert!(!json_payloads[0].is_empty(), "rollback should restore response JSON");
                assert_eq!(msgpack_payloads.len(), responses.len());
                let json_decoded: ReadMultipleResp =
                    serde_json::from_str(&json_payloads[0]).expect("json read multiple response should decode");
                assert_eq!(json_decoded.file, responses[0].file);
            },
        );
    }

    #[test]
    fn encode_batch_read_version_response_payloads_keeps_json_and_msgpack_in_sync() {
        let responses = vec![BatchReadVersionResp {
            index: 3,
            path: "object-a".to_string(),
            version_id: "version-a".to_string(),
            success: false,
            error: DiskError::FileVersionNotFound.to_string(),
            error_code: DiskError::FileVersionNotFound.to_u32(),
            ..Default::default()
        }];

        let (json_payloads, msgpack_payloads) =
            encode_batch_read_version_response_payloads(&responses, false).expect("batch read version responses should encode");

        assert_eq!(json_payloads.len(), responses.len());
        assert_eq!(msgpack_payloads.len(), responses.len());

        let json_decoded: BatchReadVersionResp =
            serde_json::from_str(&json_payloads[0]).expect("json batch read version response should decode");
        let msgpack_decoded = decode_msgpack_or_json::<BatchReadVersionResp>(&msgpack_payloads[0], "", "BatchReadVersionResp")
            .expect("msgpack batch read version response should decode");

        assert_eq!(json_decoded.index, responses[0].index);
        assert_eq!(json_decoded.error_code, responses[0].error_code);
        assert_eq!(msgpack_decoded.path, responses[0].path);
        assert_eq!(msgpack_decoded.error, responses[0].error);
        assert_eq!(msgpack_decoded.error_code, responses[0].error_code);
    }

    #[test]
    fn batch_read_version_response_decode_accepts_legacy_payload_without_error_code() {
        #[derive(Serialize)]
        struct LegacyBatchReadVersionResp {
            index: usize,
            path: String,
            version_id: String,
            success: bool,
            file_info: FileInfo,
            error: String,
        }

        let legacy = LegacyBatchReadVersionResp {
            index: 2,
            path: "object-legacy".to_string(),
            version_id: "version-legacy".to_string(),
            success: false,
            file_info: FileInfo::default(),
            error: "legacy error".to_string(),
        };
        let legacy_json = serde_json::to_string(&legacy).expect("legacy json should encode");
        let legacy_msgpack = encode_msgpack(&legacy, "LegacyBatchReadVersionResp").expect("legacy msgpack should encode");

        let json_decoded: BatchReadVersionResp =
            decode_msgpack_or_json(&[], &legacy_json, "BatchReadVersionResp").expect("legacy json should decode");
        let msgpack_decoded: BatchReadVersionResp =
            decode_msgpack_or_json(&legacy_msgpack, "", "BatchReadVersionResp").expect("legacy msgpack should decode");

        assert_eq!(json_decoded.error_code, 0);
        assert_eq!(msgpack_decoded.error_code, 0);
        assert_eq!(msgpack_decoded.error, legacy.error);
    }

    #[test]
    fn encode_batch_read_version_response_payloads_respects_msgpack_only_gate_and_rollback() {
        let responses = vec![BatchReadVersionResp {
            index: 4,
            path: "object-b".to_string(),
            version_id: "version-b".to_string(),
            success: true,
            ..Default::default()
        }];

        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("true")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, Some("true")),
            ],
            || {
                let (json_payloads, msgpack_payloads) = encode_batch_read_version_response_payloads(&responses, false)
                    .expect("batch read version responses should encode");

                assert_eq!(json_payloads, vec![String::new()]);
                assert_eq!(msgpack_payloads.len(), responses.len());
                let decoded = decode_msgpack_or_json::<BatchReadVersionResp>(&msgpack_payloads[0], "", "BatchReadVersionResp")
                    .expect("msgpack batch read version response should decode");
                assert_eq!(decoded.path, responses[0].path);
            },
        );

        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, Some("false")),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, Some("true")),
            ],
            || {
                let (json_payloads, msgpack_payloads) = encode_batch_read_version_response_payloads(&responses, false)
                    .expect("batch read version responses should encode");

                assert!(!json_payloads[0].is_empty(), "rollback should restore response JSON");
                assert_eq!(msgpack_payloads.len(), responses.len());
                let json_decoded: BatchReadVersionResp =
                    serde_json::from_str(&json_payloads[0]).expect("json batch read version response should decode");
                assert_eq!(json_decoded.path, responses[0].path);
            },
        );
    }

    #[test]
    fn encode_batch_read_version_response_payloads_omits_json_for_msgpack_decoded_request() {
        let responses = vec![BatchReadVersionResp {
            index: 5,
            path: "object-c".to_string(),
            version_id: "version-c".to_string(),
            success: true,
            ..Default::default()
        }];

        with_internode_msgpack_env(
            [
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY, None::<&str>),
                (rustfs_config::ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED, None::<&str>),
            ],
            || {
                let (json_payloads, msgpack_payloads) = encode_batch_read_version_response_payloads(&responses, true)
                    .expect("batch read version responses should encode");

                assert_eq!(json_payloads, vec![String::new()]);
                assert_eq!(msgpack_payloads.len(), responses.len());
                let decoded = decode_msgpack_or_json::<BatchReadVersionResp>(&msgpack_payloads[0], "", "BatchReadVersionResp")
                    .expect("msgpack batch read version response should decode");
                assert_eq!(decoded.path, responses[0].path);
            },
        );
    }
}

#[cfg(test)]
mod legacy_artifact_tests {
    use super::*;
    use crate::storage::storage_api::ecstore_disk;
    use crate::storage::storage_api::set_tonic_canonical_body_digest;
    use rustfs_iam::store::{
        Store as _,
        object::{IAM_CONFIG_PREFIX, ObjectStore},
    };
    use rustfs_kms::KmsServiceManager;
    use std::sync::Arc;

    fn mark_v2_authenticated<T>(request: &mut Request<T>) {
        request
            .metadata_mut()
            .insert("x-rustfs-rpc-auth-version", "2".parse().expect("valid v2 metadata"));
    }

    fn write_request(disk: &str, volume: &str, path: &str) -> Request<WriteAllRequest> {
        let mut request = Request::new(WriteAllRequest {
            disk: disk.to_owned(),
            volume: volume.to_owned(),
            path: path.to_owned(),
            data: Bytes::from_static(b"legacy receiver bytes"),
        });
        let body = rustfs_protos::canonical_write_all_request_body(request.get_ref()).expect("canonical write");
        set_tonic_canonical_body_digest(&mut request, &body).expect("write digest");
        mark_v2_authenticated(&mut request);
        request
    }

    async fn service_for(env: &rustfs_test_utils::TestECStoreEnv) -> NodeService {
        ObjectStore::new(env.ecstore.clone())
            .save_iam_config(serde_json::json!({"version":1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
            .await
            .expect("IAM format");
        let iam = rustfs_iam::build_iam_sys(env.ecstore.clone()).await.expect("fixture IAM");
        let context = Arc::new(crate::runtime_sources::AppContext::with_default_interfaces(
            env.ecstore.clone(),
            iam,
            Arc::new(KmsServiceManager::new()),
        ));
        super::super::make_server_for_context(Some(context))
    }

    async fn authenticated_volume_create(
        service: &NodeService,
        disk: &str,
        volumes: &[&str],
        batch: bool,
        tamper: bool,
    ) -> Result<Result<(), DiskError>, Status> {
        let (success, error) = if batch {
            let mut request = Request::new(MakeVolumesRequest {
                disk: disk.to_owned(),
                volumes: volumes.iter().map(|volume| (*volume).to_owned()).collect(),
            });
            let body = rustfs_protos::canonical_make_volumes_request_body(request.get_ref()).expect("batch body");
            sign_legacy_rpc_request(&mut request, &body, "MakeVolumes");
            if tamper {
                request.get_mut().volumes.push("tampered-volume".to_owned());
            }
            verify_legacy_rpc_request(&request, "MakeVolumes")?;
            let response = service.handle_make_volumes(request).await?.into_inner();
            (response.success, response.error)
        } else {
            let mut request = Request::new(MakeVolumeRequest {
                disk: disk.to_owned(),
                volume: volumes[0].to_owned(),
            });
            let body = rustfs_protos::canonical_make_volume_request_body(request.get_ref()).expect("single body");
            sign_legacy_rpc_request(&mut request, &body, "MakeVolume");
            if tamper {
                request.get_mut().volume = "tampered-volume".to_owned();
            }
            verify_legacy_rpc_request(&request, "MakeVolume")?;
            let response = service.handle_make_volume(request).await?.into_inner();
            (response.success, response.error)
        };
        assert_eq!(success, error.is_none(), "volume response envelope");
        Ok(error.map_or(Ok(()), |error| Err(error.into())))
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_volume_create_receiver_isolates_instances_and_slot() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-volume-create-secret".to_owned());
        let root_a = tempfile::tempdir().expect("A root");
        let env_a = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root_a.path())
            .build()
            .await;
        let ready_a = service_for(&env_a).await;
        let root_b = tempfile::tempdir().expect("B root");
        let instance_b = crate::storage::storage_api::new_instance_ctx();
        let env_b = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root_b.path())
            .instance_ctx(instance_b.clone())
            .init_bucket_metadata(false)
            .build()
            .await;
        let ready_b = service_for(&env_b).await;
        let disk_a = env_a.disk_paths[0].to_string_lossy().into_owned();
        let disk_b = env_b.disk_paths[0].to_string_lossy().into_owned();
        let disk = ready_a
            .local_mutation_target()
            .find_mutation_disk(&disk_a)
            .await
            .expect("A disk");
        let id_a = ecstore_disk::DiskAPI::get_disk_id(disk.as_ref())
            .await
            .expect("real A disk ID")
            .expect("formatted A")
            .to_string();
        // Reopen the second physical root with the same persisted identity, without reusing A's disk object.
        tokio::fs::copy(
            env_a.disk_paths[0].join(".rustfs.sys/format.json"),
            env_b.disk_paths[0].join(".rustfs.sys/format.json"),
        )
        .await
        .expect("copy real disk identity");
        crate::storage::storage_api::init_local_disks_with_instance_ctx(&instance_b, env_b.endpoint_pools.clone())
            .await
            .expect("reopen B instance registry");
        let same_id_b = ready_b
            .local_mutation_target()
            .find_mutation_disk(&disk_b)
            .await
            .expect("reopened real B disk");
        assert_eq!(
            ecstore_disk::DiskAPI::get_disk_id(same_id_b.as_ref())
                .await
                .expect("real B disk ID")
                .expect("formatted B")
                .to_string(),
            id_a,
            "both independent roots expose the same disk UUID"
        );
        let slot = super::super::ServerContextSlot::with_instance_context(instance_b.clone());
        let mut slot_b = super::super::make_server_for_context(ready_a.context.clone());
        slot_b.server_ctx = Some(slot.clone());
        let unbound = super::super::make_server_for_slot(super::super::ServerContextSlot::new());
        for batch in [false, true] {
            for (index, service) in [&ready_b, &slot_b, &unbound].into_iter().enumerate() {
                let volume = format!("foreign-{batch}-{index}-{}", Uuid::new_v4());
                let error = authenticated_volume_create(service, &disk_a, &[&volume], batch, false)
                    .await
                    .expect("foreign envelope")
                    .expect_err("receiver cannot use ambient A disk");
                assert_eq!(error.to_string(), "io error io error cannot find disk");
                assert!(!env_a.disk_paths[0].join(&volume).exists(), "foreign receiver must not mutate A");
                assert!(!env_b.disk_paths[0].join(&volume).exists(), "foreign disk reference must not create on B");
            }
            let volume = format!("unbound-{}", Uuid::new_v4());
            let error = authenticated_volume_create(&unbound, &id_a, &[&volume], batch, false)
                .await
                .expect("unbound envelope")
                .expect_err("unbound receiver cannot resolve a globally known UUID");
            assert_eq!(error.to_string(), "io error io error cannot find disk");
            assert!(!env_a.disk_paths[0].join(&volume).exists());
            assert!(!env_b.disk_paths[0].join(&volume).exists());
            for (service, own, other) in [
                (&ready_a, &env_a.disk_paths[0], &env_b.disk_paths[0]),
                (&ready_b, &env_b.disk_paths[0], &env_a.disk_paths[0]),
                (&slot_b, &env_b.disk_paths[0], &env_a.disk_paths[0]),
            ] {
                let volume = format!("same-uuid-{}", Uuid::new_v4());
                authenticated_volume_create(service, &id_a, &[&volume], batch, false)
                    .await
                    .expect("shared UUID envelope")
                    .expect("captured instance resolves its own same-UUID disk");
                assert!(own.join(&volume).is_dir(), "captured instance must create on its own physical root");
                assert!(
                    !other.join(&volume).exists(),
                    "same UUID must never select another instance's physical root"
                );
            }
            let volume = format!("bootstrap-b-{}", Uuid::new_v4());
            authenticated_volume_create(&slot_b, &disk_b, &[&volume], batch, false)
                .await
                .expect("B Bootstrap envelope")
                .expect("slot overrides ambient Ready context");
            assert!(env_b.disk_paths[0].join(&volume).is_dir());
            assert!(!env_a.disk_paths[0].join(&volume).exists());
        }
        slot.try_install(ready_b.context.clone().expect("B context"))
            .expect("install B Ready");
        for batch in [false, true] {
            for disk_ref in [&disk_b, &id_a] {
                let volume = format!("ready-b-{}", Uuid::new_v4());
                authenticated_volume_create(&slot_b, disk_ref, &[&volume], batch, false)
                    .await
                    .expect("B Ready envelope")
                    .expect("Ready slot creates on B");
                assert!(env_b.disk_paths[0].join(&volume).is_dir());
                assert!(!env_a.disk_paths[0].join(&volume).exists());
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_volume_create_receiver_preserves_bootstrap_and_batch_semantics() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-volume-create-secret".to_owned());
        let root = tempfile::tempdir().expect("unformatted startup disk");
        let instance = crate::storage::storage_api::new_instance_ctx();
        use crate::storage::storage_api::{
            Endpoint, EndpointServerPools, Endpoints, PoolEndpoints, init_local_disks_with_instance_ctx,
        };
        let mut endpoint = Endpoint::try_from(root.path().to_str().expect("disk path")).expect("startup endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        init_local_disks_with_instance_ctx(
            &instance,
            EndpointServerPools::from(vec![PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: 1,
                endpoints: Endpoints::from(vec![endpoint.clone()]),
                cmd_line: "volume-create-startup-test".to_owned(),
                platform: "test".to_owned(),
            }]),
        )
        .await
        .expect("register unformatted startup disk");
        let service = super::super::make_server_for_slot(super::super::ServerContextSlot::with_instance_context(instance));
        let disk_ref = endpoint.to_string();
        assert!(
            !root.path().join(".rustfs.sys/format.json").exists(),
            "startup fixture has no disk identity yet"
        );
        for (volume, expected) in [
            (".minio.sys", Ok(())),
            (".rustfs.sys", Err(DiskError::VolumeExists)),
            ("ordinary-startup", Ok(())),
        ] {
            let result = authenticated_volume_create(&service, &disk_ref, &[volume], false, false)
                .await
                .expect("startup envelope");
            assert_eq!(result, expected, "unformatted startup preserves precreated data-usage volume");
            assert!(root.path().join(volume).is_dir());
        }
        let error = authenticated_volume_create(&service, &disk_ref, &["ordinary-startup"], false, false)
            .await
            .expect("duplicate envelope")
            .expect_err("single duplicate remains typed");
        assert_eq!(error, DiskError::VolumeExists);
        authenticated_volume_create(&service, &disk_ref, &["ordinary-startup", "batch-new", "batch-new"], true, false)
            .await
            .expect("batch envelope")
            .expect("batch ignores duplicate volumes");
        assert!(root.path().join("batch-new").is_dir());
        authenticated_volume_create(&service, &disk_ref, &[], true, false)
            .await
            .expect("empty envelope")
            .expect("empty batch");
        let error = authenticated_volume_create(&service, &disk_ref, &["batch-first", "x", "batch-last"], true, false)
            .await
            .expect("partial batch envelope")
            .expect_err("batch stops at first invalid volume");
        assert_eq!(error.to_string(), "io error io error Invalid arguments specified");
        assert!(root.path().join("batch-first").is_dir(), "prior successful item remains");
        assert!(!root.path().join("x").exists());
        assert!(!root.path().join("batch-last").exists(), "later item must not execute");
        for batch in [false, true] {
            let error = authenticated_volume_create(&service, &disk_ref, &["signed-only"], batch, true)
                .await
                .expect_err("body changes must be rejected");
            assert_eq!(error.code(), tonic::Code::PermissionDenied);
            assert!(!root.path().join("signed-only").exists());
            assert!(!root.path().join("tampered-volume").exists());
        }
        assert!(!root.path().join(".rustfs.sys/format.json").exists());
    }

    const LEGACY_RENAME_AUDIENCE: &str = "127.0.0.1:24800";
    const LEGACY_RENAME_BODY: &[u8] = b"authenticated real inline rename body";

    fn inline_rename_file_info(path: &str) -> FileInfo {
        let mut fi = FileInfo::new(path, 1, 0);
        fi.erasure.index = 1;
        fi.version_id = Some(Uuid::new_v4());
        fi.mod_time = Some(time::OffsetDateTime::now_utc());
        fi.size = i64::try_from(LEGACY_RENAME_BODY.len()).expect("inline fixture size");
        fi.parts = vec![rustfs_filemeta::ObjectPartInfo {
            number: 1,
            size: LEGACY_RENAME_BODY.len(),
            actual_size: fi.size,
            ..Default::default()
        }];
        fi.data = Some(Bytes::from_static(LEGACY_RENAME_BODY));
        fi.set_inline_data();
        fi.validate_for_metadata_read().expect("real inline metadata");
        fi
    }

    fn signed_legacy_rename_request(
        disk: &str,
        source: (&str, &str),
        destination: (&str, &str),
        fi: &FileInfo,
        msgpack: bool,
    ) -> Request<RenameDataRequest> {
        let mut request = Request::new(RenameDataRequest {
            disk: disk.to_owned(),
            src_volume: source.0.to_owned(),
            src_path: source.1.to_owned(),
            dst_volume: destination.0.to_owned(),
            dst_path: destination.1.to_owned(),
            file_info: if msgpack {
                "invalid JSON must not be decoded".to_owned()
            } else {
                serde_json::to_string(fi).expect("JSON FI")
            },
            file_info_bin: if msgpack {
                encode_file_info_msgpack(fi).expect("msgpack FI").into()
            } else {
                Bytes::new()
            },
            ..Default::default()
        });
        let body = rustfs_protos::canonical_rename_data_request_body(request.get_ref()).expect("canonical rename body");
        sign_legacy_rpc_request(&mut request, &body, "RenameData");
        request
    }

    fn sign_legacy_rpc_request<T>(request: &mut Request<T>, body: &[u8], rpc_method: &str) {
        use crate::storage::storage_api::ecstore_rpc as rpc;
        set_tonic_canonical_body_digest(request, body).expect("canonical mutation digest");
        let rpc_path = format!("/node_service.NodeService/{rpc_method}");
        let digest = request
            .metadata()
            .get("x-rustfs-content-sha256")
            .expect("digest metadata")
            .to_str()
            .expect("digest text");
        let mut headers =
            rpc::gen_tonic_signature_headers(LEGACY_RENAME_AUDIENCE, "node_service.NodeService", rpc_method, Some(digest))
                .expect("real HMAC signature");
        let challenge = Uuid::new_v4();
        let epoch_proof = rpc::tonic_boot_epoch_response_headers(LEGACY_RENAME_AUDIENCE, challenge).expect("signed epoch proof");
        let epoch =
            rpc::verify_tonic_boot_epoch_response(LEGACY_RENAME_AUDIENCE, challenge, &epoch_proof).expect("authenticated epoch");
        let replay = rpc::gen_tonic_replay_scope_headers(
            LEGACY_RENAME_AUDIENCE,
            &rpc_path,
            headers["x-rustfs-timestamp"].to_str().expect("signed timestamp"),
            headers["x-rustfs-content-sha256"].to_str().expect("signed digest"),
            epoch,
        )
        .expect("real replay-scoped signature");
        headers.extend(replay);
        request.metadata_mut().as_mut().extend(headers);
    }

    fn verify_legacy_rpc_request<T>(request: &Request<T>, rpc_method: &str) -> Result<(), Status> {
        let rpc_path = format!("/node_service.NodeService/{rpc_method}");
        // Use the same HMAC/replay verifier as production check_auth before
        // the handler independently verifies its canonical body digest.
        crate::storage::storage_api::ecstore_rpc::verify_tonic_rpc_signature_with_bootstrap(
            LEGACY_RENAME_AUDIENCE,
            &rpc_path,
            request.metadata().as_ref(),
            false,
        )
        .map_err(|_| Status::unauthenticated("legacy RPC signature rejected"))
    }

    async fn authenticated_legacy_rename(
        service: &NodeService,
        request: Request<RenameDataRequest>,
    ) -> Result<Response<RenameDataResponse>, Status> {
        verify_legacy_rpc_request(&request, "RenameData")?;
        service.handle_rename_data(request, false).await
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_rename_data_receiver_preserves_authenticated_inline_versions_and_rejects_managed_volumes() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-rename-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("rename receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        for bucket in ["rename-ordinary-a", "rename-ordinary-b"] {
            env.make_bucket(bucket, true).await;
        }
        env.ecstore
            .clone()
            .make_artifact_bucket("rename-managed", "outputs/")
            .await
            .expect("managed bucket");
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let target = service.local_mutation_target();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("listener local disk");

        let fi = inline_rename_file_info("bad-auth-destination");
        ecstore_disk::DiskAPI::write_metadata(disk.as_ref(), "", "rename-ordinary-a", "bad-auth-source", fi.clone())
            .await
            .expect("real auth negative source");
        let mut bad_auth = signed_legacy_rename_request(
            &disk_ref,
            ("rename-ordinary-a", "bad-auth-source"),
            ("rename-ordinary-b", "bad-auth-destination"),
            &fi,
            false,
        );
        bad_auth
            .metadata_mut()
            .insert("x-rustfs-rpc-signature-v3", "00".repeat(32).parse().expect("invalid HMAC metadata"));
        let error = authenticated_legacy_rename(&service, bad_auth)
            .await
            .expect_err("bad HMAC must fail before the handler");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert!(!env.disk_paths[0].join("rename-ordinary-b/bad-auth-destination").exists());

        for msgpack in [false, true] {
            for (case, (source_volume, destination_volume)) in [
                ("rename-ordinary-a", "rename-ordinary-a"),
                ("rename-ordinary-a", "rename-ordinary-b"),
                (".rustfs.sys", ".rustfs.sys"),
                (".rustfs.sys/tmp", ".rustfs.sys"),
                (".rustfs.sys/tmp", "rename-ordinary-a"),
            ]
            .into_iter()
            .enumerate()
            {
                let source_path = format!("rename-source-{case}-{msgpack}");
                let destination_path = format!("rename-destination-{case}-{msgpack}");
                let fi = inline_rename_file_info(&destination_path);
                let version = fi.version_id.expect("non-nil inline version");
                ecstore_disk::DiskAPI::write_metadata(disk.as_ref(), "", source_volume, &source_path, fi.clone())
                    .await
                    .expect("real staged inline metadata");
                let response = authenticated_legacy_rename(
                    &service,
                    signed_legacy_rename_request(
                        &disk_ref,
                        (source_volume, &source_path),
                        (destination_volume, &destination_path),
                        &fi,
                        msgpack,
                    ),
                )
                .await
                .expect("authenticated rename response")
                .into_inner();
                assert!(response.success, "case={case}, msgpack={msgpack}: {:?}", response.error);
                assert!(response.error.is_none());
                let _: RenameDataResp =
                    decode_msgpack_or_json(&response.rename_data_resp_bin, &response.rename_data_resp, "RenameDataResp")
                        .expect("real rename result codec");
                let actual = disk
                    .read_version(
                        destination_volume,
                        destination_volume,
                        &destination_path,
                        &version.to_string(),
                        &ReadOptions {
                            read_data: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("read exact physical renamed version");
                assert_eq!(actual.version_id, Some(version));
                assert_eq!(actual.data, Some(Bytes::from_static(LEGACY_RENAME_BODY)));
            }
        }

        let attempt = format!("outputs/74656e616e74/{}/{}/", Uuid::new_v4(), Uuid::new_v4());
        for source_managed in [false, true] {
            let source_volume = if source_managed {
                "rename-managed"
            } else {
                "rename-ordinary-a"
            };
            let destination_volume = if source_managed {
                "rename-ordinary-b"
            } else {
                "rename-managed"
            };
            let source_path = if source_managed {
                format!("{attempt}source")
            } else {
                "managed-denial-source".to_owned()
            };
            let destination_path = if source_managed {
                "managed-source-denied".to_owned()
            } else {
                format!("{attempt}denied")
            };
            let fi = inline_rename_file_info(&destination_path);
            ecstore_disk::DiskAPI::write_metadata(disk.as_ref(), "", source_volume, &source_path, fi.clone())
                .await
                .expect("managed denial source fixture");
            let source_meta = env.disk_paths[0].join(source_volume).join(&source_path).join("xl.meta");
            let before = tokio::fs::read(&source_meta).await.expect("original source metadata");
            let error = authenticated_legacy_rename(
                &service,
                signed_legacy_rename_request(
                    &disk_ref,
                    (source_volume, &source_path),
                    (destination_volume, &destination_path),
                    &fi,
                    source_managed,
                ),
            )
            .await
            .expect_err("managed source or destination must reject legacy rename");
            assert_eq!(error.code(), tonic::Code::PermissionDenied);
            assert_eq!(tokio::fs::read(&source_meta).await.expect("denied source retained"), before);
            assert!(
                !env.disk_paths[0].join(destination_volume).join(&destination_path).exists(),
                "managed denial must not publish or create destination parents"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_rename_data_receiver_rejects_missing_physical_volumes_with_retained_metadata() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-rename-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("missing rename receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let target = service.local_mutation_target();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("listener local disk");
        for missing_source in [false, true] {
            let source_volume = if missing_source { "missing-source" } else { "retained-source" };
            let destination_volume = if missing_source {
                "retained-destination"
            } else {
                "missing-destination"
            };
            for bucket in [source_volume, destination_volume] {
                env.make_bucket(bucket, true).await;
            }
            let source_path = "source";
            let destination_path = "nested/destination";
            let fi = inline_rename_file_info(destination_path);
            ecstore_disk::DiskAPI::write_metadata(disk.as_ref(), "", source_volume, source_path, fi.clone())
                .await
                .expect("real missing-volume source");
            let source_meta = env.disk_paths[0].join(source_volume).join(source_path).join("xl.meta");
            let before = tokio::fs::read(&source_meta)
                .await
                .expect("source metadata before physical removal");
            let missing = if missing_source { source_volume } else { destination_volume };
            let incarnation = env
                .ecstore
                .bucket_incarnation_id(missing)
                .await
                .expect("authoritative incarnation before removal");
            disk.delete_volume(missing, true)
                .await
                .expect("remove only the receiver physical volume");
            assert_eq!(
                env.ecstore
                    .bucket_incarnation_id(missing)
                    .await
                    .expect("bucket metadata retained"),
                incarnation
            );
            target
                .reject_legacy_artifact_mutation(missing)
                .await
                .expect("ordinary authority still classifies retained metadata");
            let response = authenticated_legacy_rename(
                &service,
                signed_legacy_rename_request(
                    &disk_ref,
                    (source_volume, source_path),
                    (destination_volume, destination_path),
                    &fi,
                    missing_source,
                ),
            )
            .await
            .expect("missing-volume rename response")
            .into_inner();
            assert!(!response.success, "missing source or destination must reject admission");
            assert_eq!(
                DiskError::from(response.error.expect("explicit missing-volume error")),
                DiskError::VolumeNotFound
            );
            assert!(
                !env.disk_paths[0].join(missing).exists(),
                "retained metadata must not revive the missing physical volume"
            );
            assert!(
                !env.disk_paths[0].join(destination_volume).join(destination_path).exists(),
                "missing-volume rename must not publish a destination"
            );
            if !missing_source {
                assert_eq!(tokio::fs::read(source_meta).await.expect("rejected source retained"), before);
            }
        }
    }

    fn signed_legacy_update_metadata_request(
        disk: &str,
        volume: &str,
        path: &str,
        fi: &FileInfo,
        opts: &UpdateMetadataOpts,
        msgpack: bool,
    ) -> Request<UpdateMetadataRequest> {
        let mut request = Request::new(UpdateMetadataRequest {
            disk: disk.to_owned(),
            volume: volume.to_owned(),
            path: path.to_owned(),
            file_info: if msgpack {
                "invalid JSON must not be decoded".to_owned()
            } else {
                serde_json::to_string(fi).expect("JSON FI")
            },
            opts: if msgpack {
                "invalid JSON must not be decoded".to_owned()
            } else {
                serde_json::to_string(opts).expect("JSON update options")
            },
            file_info_bin: if msgpack {
                encode_file_info_msgpack(fi).expect("msgpack FI").into()
            } else {
                Bytes::new()
            },
            opts_bin: if msgpack {
                encode_msgpack(opts, "UpdateMetadataOpts")
                    .expect("msgpack update options")
                    .into()
            } else {
                Bytes::new()
            },
        });
        let body = rustfs_protos::canonical_update_metadata_request_body(request.get_ref()).expect("canonical update body");
        sign_legacy_rpc_request(&mut request, &body, "UpdateMetadata");
        request
    }

    async fn authenticated_legacy_update_metadata(
        service: &NodeService,
        request: Request<UpdateMetadataRequest>,
    ) -> Result<Response<UpdateMetadataResponse>, Status> {
        verify_legacy_rpc_request(&request, "UpdateMetadata")?;
        service.handle_update_metadata(request).await
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_update_metadata_receiver_preserves_ordinary_internal_and_managed_semantics() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-update-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("update receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("update-ordinary", true).await;
        env.ecstore
            .clone()
            .make_artifact_bucket("update-managed", "outputs/")
            .await
            .expect("managed bucket");
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let target = service.local_mutation_target();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("listener local disk");
        for msgpack in [false, true] {
            for volume in ["update-ordinary", ".rustfs.sys", "update-managed"] {
                let path = if volume == "update-managed" {
                    format!("outputs/74656e616e74/{}/{}/object-{msgpack}", Uuid::new_v4(), Uuid::new_v4())
                } else {
                    format!("legacy-update/object-{msgpack}")
                };
                let mut original = inline_rename_file_info(&path);
                original.fresh = false;
                original.metadata.insert("keep".to_owned(), "original".to_owned());
                original.metadata.insert("changed".to_owned(), "before".to_owned());
                ecstore_disk::DiskAPI::write_metadata(disk.as_ref(), "", volume, &path, original.clone())
                    .await
                    .expect("real original inline version");
                let mut history = original.clone();
                history.version_id = Some(Uuid::new_v4());
                history.metadata.clear();
                history.set_inline_data();
                history.metadata.insert("history".to_owned(), "untouched".to_owned());
                ecstore_disk::DiskAPI::write_metadata(disk.as_ref(), "", volume, &path, history.clone())
                    .await
                    .expect("second real inline version");
                let version = original.version_id.expect("original version");
                let history_version = history.version_id.expect("history version");
                let history_before = disk
                    .read_version(
                        volume,
                        volume,
                        &path,
                        &history_version.to_string(),
                        &ReadOptions {
                            read_data: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("original historical version");
                let physical = env.disk_paths[0].join(volume).join(&path).join("xl.meta");
                let before = tokio::fs::read(&physical).await.expect("real xl.meta");
                for replace in [false, true] {
                    let mut update = original.clone();
                    update.metadata.clear();
                    update.set_inline_data();
                    update
                        .metadata
                        .insert("changed".to_owned(), if replace { "replaced" } else { "merged" }.to_owned());
                    update.mod_time = Some(time::OffsetDateTime::now_utc());
                    // UpdateMetadata must preserve stored bytes and geometry,
                    // even when the incoming FileInfo carries a different body.
                    let incoming = b"incoming metadata must not replace the stored body";
                    update.data = Some(Bytes::from_static(incoming));
                    update.size = i64::try_from(incoming.len()).expect("incoming size");
                    update.parts[0].size = incoming.len();
                    update.parts[0].actual_size = update.size;
                    update.validate_for_metadata_read().expect("valid incoming inline FI");
                    let opts = UpdateMetadataOpts {
                        replace_user_metadata: replace,
                        no_persistence: replace,
                        transition_reconcile: None,
                    };
                    let result = authenticated_legacy_update_metadata(
                        &service,
                        signed_legacy_update_metadata_request(&disk_ref, volume, &path, &update, &opts, msgpack),
                    )
                    .await;
                    if volume == "update-managed" {
                        assert_eq!(
                            result.expect_err("managed metadata update must reject").code(),
                            tonic::Code::PermissionDenied
                        );
                        assert_eq!(tokio::fs::read(&physical).await.expect("managed metadata retained"), before);
                        continue;
                    }
                    let response = result.expect("authenticated update response").into_inner();
                    assert!(
                        response.success,
                        "volume={volume}, msgpack={msgpack}, replace={replace}: {:?}",
                        response.error
                    );
                    assert!(response.error.is_none());
                    let actual = disk
                        .read_version(
                            volume,
                            volume,
                            &path,
                            &version.to_string(),
                            &ReadOptions {
                                read_data: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("exact updated version");
                    assert_eq!(actual.version_id, Some(version));
                    assert_eq!(actual.data, Some(Bytes::from_static(LEGACY_RENAME_BODY)));
                    assert_eq!(actual.size, original.size);
                    assert_eq!(actual.mod_time, update.mod_time);
                    assert_eq!(
                        actual.metadata.get("changed").map(String::as_str),
                        Some(if replace { "replaced" } else { "merged" })
                    );
                    assert_eq!(
                        actual.metadata.get("keep").map(String::as_str),
                        if replace { None } else { Some("original") }
                    );
                    let historical = disk
                        .read_version(
                            volume,
                            volume,
                            &path,
                            &history_version.to_string(),
                            &ReadOptions {
                                read_data: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("untouched historical version");
                    assert_eq!(historical.version_id, Some(history_version));
                    assert_eq!(historical.data, history_before.data);
                    assert_eq!(historical.metadata, history_before.metadata);
                    assert_eq!(historical.mod_time, history_before.mod_time);
                }
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_update_metadata_receiver_rejects_missing_volume_with_retained_metadata() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-update-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("missing update receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let target = service.local_mutation_target();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("listener local disk");
        for msgpack in [false, true] {
            let volume = if msgpack {
                "update-missing-msgpack"
            } else {
                "update-missing-json"
            };
            env.make_bucket(volume, true).await;
            let path = "nested/object";
            let original = inline_rename_file_info(path);
            ecstore_disk::DiskAPI::write_metadata(disk.as_ref(), "", volume, path, original.clone())
                .await
                .expect("real metadata before removal");
            let incarnation = env
                .ecstore
                .bucket_incarnation_id(volume)
                .await
                .expect("authoritative incarnation");
            disk.delete_volume(volume, true)
                .await
                .expect("remove only the receiver physical volume");
            assert_eq!(env.ecstore.bucket_incarnation_id(volume).await.expect("retained authority"), incarnation);
            target
                .reject_legacy_artifact_mutation(volume)
                .await
                .expect("retained ordinary authority");
            let mut update = original;
            update.metadata.insert("changed".to_owned(), "must-not-publish".to_owned());
            let opts = UpdateMetadataOpts {
                no_persistence: msgpack,
                replace_user_metadata: msgpack,
                transition_reconcile: None,
            };
            let response = authenticated_legacy_update_metadata(
                &service,
                signed_legacy_update_metadata_request(&disk_ref, volume, path, &update, &opts, msgpack),
            )
            .await
            .expect("missing-volume update response")
            .into_inner();
            assert!(!response.success, "missing physical volume must reject update admission");
            assert_eq!(
                DiskError::from(response.error.expect("precise missing-volume error")),
                DiskError::VolumeNotFound
            );
            assert!(
                !env.disk_paths[0].join(volume).exists(),
                "retained authority cannot revive a missing volume or object"
            );
        }
    }

    fn signed_legacy_delete_request(
        disk: &str,
        volume: &str,
        path: &str,
        fi: &FileInfo,
        opts: &DeleteOptions,
        msgpack: bool,
        force_del_marker: bool,
    ) -> Request<DeleteVersionRequest> {
        let mut request = Request::new(DeleteVersionRequest {
            disk: disk.to_owned(),
            volume: volume.to_owned(),
            path: path.to_owned(),
            force_del_marker,
            file_info: if msgpack {
                "invalid JSON must not be decoded".to_owned()
            } else {
                serde_json::to_string(fi).expect("JSON FI")
            },
            opts: if msgpack {
                "invalid JSON must not be decoded".to_owned()
            } else {
                serde_json::to_string(opts).expect("JSON delete options")
            },
            file_info_bin: if msgpack {
                encode_file_info_msgpack(fi).expect("msgpack FI").into()
            } else {
                Bytes::new()
            },
            opts_bin: if msgpack {
                encode_msgpack_named(opts, "DeleteOptions")
                    .expect("msgpack delete options")
                    .into()
            } else {
                Bytes::new()
            },
            ..Default::default()
        });
        let body = rustfs_protos::canonical_delete_version_request_body(request.get_ref()).expect("canonical delete body");
        sign_legacy_rpc_request(&mut request, &body, "DeleteVersion");
        request
    }

    async fn authenticated_legacy_delete(
        service: &NodeService,
        request: Request<DeleteVersionRequest>,
    ) -> Result<Response<DeleteVersionResponse>, Status> {
        verify_legacy_rpc_request(&request, "DeleteVersion")?;
        service.handle_delete_version(request, false).await
    }

    #[derive(Clone, Copy, Debug)]
    enum LegacyPathCase {
        Delete,
        DeletePaths,
        Residual(bool),
    }

    impl LegacyPathCase {
        const ALL: [Self; 4] = [Self::Delete, Self::DeletePaths, Self::Residual(false), Self::Residual(true)];
    }

    struct LegacyPathFixture {
        case: LegacyPathCase,
        volume: String,
        path: String,
        physical: std::path::PathBuf,
        fi: FileInfo,
        previous: Option<FileInfo>,
        options: DeleteOptions,
        scanner: Option<Uuid>,
    }

    async fn legacy_path_fixture(disk: &ecstore_disk::DiskStore, volume: &str, case: LegacyPathCase) -> LegacyPathFixture {
        let path = format!("outputs/74656e616e74/{}/{}", Uuid::new_v4(), Uuid::new_v4());
        let physical = ecstore_disk::DiskAPI::path(disk.as_ref()).join(volume).join(&path);
        let fi = inline_rename_file_info(&path);
        let (previous, options) = match case {
            LegacyPathCase::Residual(undo_write) => {
                let mut previous = fi.clone();
                previous.version_id = Some(Uuid::new_v4());
                previous.mod_time = fi.mod_time.map(|value| value - time::Duration::seconds(1));
                ecstore_disk::DiskAPI::write_metadata(disk.as_ref(), "", volume, &path, previous.clone())
                    .await
                    .expect("retained inline history");
                ecstore_disk::DiskAPI::write_metadata(disk.as_ref(), "", volume, &path, fi.clone())
                    .await
                    .expect("version selected for residual deletion");
                (
                    Some(previous),
                    DeleteOptions {
                        undo_write,
                        undo_delete: true,
                        ..Default::default()
                    },
                )
            }
            _ => {
                tokio::fs::create_dir_all(&physical).await.expect("delete fixture directory");
                tokio::fs::write(physical.join("payload"), LEGACY_RENAME_BODY)
                    .await
                    .expect("delete fixture bytes");
                (
                    None,
                    DeleteOptions {
                        recursive: true,
                        immediate: true,
                        ..Default::default()
                    },
                )
            }
        };
        LegacyPathFixture {
            case,
            volume: volume.to_owned(),
            path,
            physical,
            fi,
            previous,
            options,
            scanner: None,
        }
    }

    async fn legacy_residual_file_fixture(disk: &ecstore_disk::DiskStore, volume: &str, undo_write: bool) -> LegacyPathFixture {
        let path = format!("outputs/74656e616e74/{}/{}", Uuid::new_v4(), Uuid::new_v4());
        let physical = ecstore_disk::DiskAPI::path(disk.as_ref()).join(volume).join(&path);
        tokio::fs::create_dir_all(physical.parent().expect("file parent"))
            .await
            .expect("raw file parent");
        tokio::fs::write(&physical, LEGACY_RENAME_BODY).await.expect("raw file bytes");
        LegacyPathFixture {
            case: LegacyPathCase::Residual(undo_write),
            volume: volume.to_owned(),
            fi: inline_rename_file_info(&path),
            path: format!("/{path}"),
            physical,
            previous: None,
            options: DeleteOptions {
                undo_write,
                undo_delete: true,
                ..Default::default()
            },
            scanner: None,
        }
    }

    fn path_delete_snapshot(root: &std::path::Path) -> Vec<(std::path::PathBuf, Option<Vec<u8>>)> {
        let mut paths = vec![root.to_path_buf()];
        let mut entries = Vec::new();
        while let Some(path) = paths.pop() {
            if !path.exists() {
                continue;
            }
            if path.is_dir() {
                entries.push((path.clone(), None));
                paths.extend(
                    std::fs::read_dir(path)
                        .expect("snapshot directory")
                        .map(|entry| entry.expect("snapshot entry").path()),
                );
            } else {
                entries.push((path.clone(), Some(std::fs::read(path).expect("snapshot bytes"))));
            }
        }
        entries.sort();
        entries
    }

    fn signed_path_delete_request(disk: &str, fixture: &LegacyPathFixture) -> Request<DeleteRequest> {
        let mut request = Request::new(DeleteRequest {
            disk: disk.to_owned(),
            volume: fixture.volume.clone(),
            path: fixture.path.clone(),
            options: serde_json::to_string(&fixture.options).expect("delete options"),
            scanner_publication_lease_token: fixture
                .scanner
                .map(|token| Bytes::copy_from_slice(token.as_bytes()))
                .unwrap_or_default(),
            ..Default::default()
        });
        let body = rustfs_protos::canonical_delete_request_body(request.get_ref()).expect("canonical delete");
        sign_legacy_rpc_request(&mut request, &body, "Delete");
        request
    }

    async fn authenticated_path_mutation(
        service: &NodeService,
        disk: &str,
        fixture: &LegacyPathFixture,
        msgpack: bool,
        tamper: bool,
    ) -> Result<Result<(), DiskError>, Status> {
        let (success, error) = match fixture.case {
            LegacyPathCase::Delete => {
                let mut request = signed_path_delete_request(disk, fixture);
                if tamper {
                    request.get_mut().path.push_str("-tampered");
                }
                verify_legacy_rpc_request(&request, "Delete")?;
                let response = service.handle_delete(request).await?.into_inner();
                (response.success, response.error)
            }
            LegacyPathCase::DeletePaths => {
                let mut request = Request::new(DeletePathsRequest {
                    disk: disk.to_owned(),
                    volume: fixture.volume.clone(),
                    paths: vec![fixture.path.clone(), fixture.path.clone()],
                });
                let body = rustfs_protos::canonical_delete_paths_request_body(request.get_ref()).expect("canonical paths");
                sign_legacy_rpc_request(&mut request, &body, "DeletePaths");
                if tamper {
                    request.get_mut().paths.push("tampered".to_owned());
                }
                verify_legacy_rpc_request(&request, "DeletePaths")?;
                let response = service.handle_delete_paths(request).await?.into_inner();
                (response.success, response.error)
            }
            LegacyPathCase::Residual(_) => {
                let mut request = signed_legacy_delete_request(
                    disk,
                    &fixture.volume,
                    &fixture.path,
                    &fixture.fi,
                    &fixture.options,
                    msgpack,
                    false,
                );
                if tamper {
                    request.get_mut().path.push_str("-tampered");
                }
                let response = authenticated_legacy_delete(service, request).await?.into_inner();
                if response.success {
                    assert_eq!(response.raw_file_info, "null");
                }
                (response.success, response.error)
            }
        };
        assert_eq!(success, error.is_none(), "typed error and success agree");
        Ok(error.map(DiskError::from).map_or(Ok(()), Err))
    }

    async fn assert_path_mutation_result(disk: &ecstore_disk::DiskStore, fixture: &LegacyPathFixture) {
        if let Some(previous) = &fixture.previous {
            let retained = ecstore_disk::DiskAPI::read_version(
                disk.as_ref(),
                &fixture.volume,
                &fixture.volume,
                &fixture.path,
                &previous.version_id.expect("history version").to_string(),
                &ReadOptions {
                    read_data: true,
                    ..Default::default()
                },
            )
            .await
            .expect("history survives residual deletion");
            assert_eq!(retained.version_id, previous.version_id);
            assert_eq!(retained.data, previous.data);
            assert_eq!(
                ecstore_disk::DiskAPI::read_version(
                    disk.as_ref(),
                    &fixture.volume,
                    &fixture.volume,
                    &fixture.path,
                    &fixture.fi.version_id.expect("deleted version").to_string(),
                    &ReadOptions::default()
                )
                .await
                .expect_err("selected version removed"),
                DiskError::FileVersionNotFound
            );
        } else {
            assert!(!fixture.physical.exists(), "path bytes removed");
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_path_delete_receiver_preserves_signed_shapes_and_managed_rejection() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-path-delete-secret".to_owned());
        let root = tempfile::tempdir().expect("path receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("path-ordinary", true).await;
        env.ecstore
            .clone()
            .make_artifact_bucket("path-managed", "outputs/")
            .await
            .expect("managed bucket");
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = service
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("receiver disk");
        for case in LegacyPathCase::ALL {
            for msgpack in [false, true] {
                for volume in ["path-ordinary", ".rustfs.sys", "path-managed"] {
                    let mut fixture = legacy_path_fixture(&disk, volume, case).await;
                    if msgpack && !matches!(case, LegacyPathCase::Residual(_)) {
                        fixture.path.insert(0, '/');
                    }
                    if matches!(case, LegacyPathCase::Delete) {
                        fixture.options.immediate = msgpack;
                    }
                    if matches!(case, LegacyPathCase::Residual(false)) && msgpack {
                        fixture.options.old_data_dir = Some(Uuid::new_v4());
                    }
                    let before = path_delete_snapshot(&fixture.physical);
                    assert_eq!(
                        authenticated_path_mutation(&service, &disk_ref, &fixture, msgpack, true)
                            .await
                            .expect_err("body change rejects before mutation")
                            .code(),
                        tonic::Code::PermissionDenied
                    );
                    assert_eq!(path_delete_snapshot(&fixture.physical), before);
                    if matches!(case, LegacyPathCase::Residual(false)) && msgpack && volume != ".rustfs.sys" {
                        if volume == "path-ordinary" {
                            let rejected = authenticated_path_mutation(&service, &disk_ref, &fixture, msgpack, false)
                                .await
                                .expect("obsolete ordinary transaction envelope")
                                .expect_err("ordinary legacy transaction is unsupported");
                            assert_eq!(rejected, DiskError::UnsupportedDisk);
                            assert_eq!(
                                path_delete_snapshot(&fixture.physical),
                                before,
                                "obsolete residual transaction must not change physical state"
                            );
                        }
                        // Exercise ordinary deletion and managed classification with a legal residual request.
                        fixture.options.old_data_dir = None;
                    }
                    let result = authenticated_path_mutation(&service, &disk_ref, &fixture, msgpack, false).await;
                    if volume == "path-managed" {
                        assert_eq!(result.expect_err("managed path rejects").code(), tonic::Code::PermissionDenied);
                        assert_eq!(path_delete_snapshot(&fixture.physical), before);
                    } else {
                        result.expect("authenticated envelope").expect("legacy deletion succeeds");
                        assert_path_mutation_result(&disk, &fixture).await;
                    }
                }
            }
        }
        for undo_write in [false, true] {
            for msgpack in [false, true] {
                for volume in ["path-ordinary", ".rustfs.sys", "path-managed"] {
                    let fixture = legacy_residual_file_fixture(&disk, volume, undo_write).await;
                    let before = path_delete_snapshot(&fixture.physical);
                    let result = authenticated_path_mutation(&service, &disk_ref, &fixture, msgpack, false).await;
                    if volume == "path-managed" {
                        assert_eq!(result.expect_err("managed slash deletion rejects").code(), tonic::Code::PermissionDenied);
                        assert_eq!(path_delete_snapshot(&fixture.physical), before);
                    } else {
                        result
                            .expect("slash residual envelope")
                            .expect("legacy slash deletes a regular file");
                        assert_path_mutation_result(&disk, &fixture).await;
                    }
                }
            }
        }
        for volume in ["path-ordinary", "path-managed"] {
            let mut request = Request::new(DeletePathsRequest {
                disk: disk_ref.clone(),
                volume: volume.to_owned(),
                paths: Vec::new(),
            });
            let body = rustfs_protos::canonical_delete_paths_request_body(request.get_ref()).expect("empty batch body");
            sign_legacy_rpc_request(&mut request, &body, "DeletePaths");
            verify_legacy_rpc_request(&request, "DeletePaths").expect("signed empty batch");
            let result = service.handle_delete_paths(request).await;
            if volume == "path-managed" {
                assert_eq!(
                    result.expect_err("empty batch cannot bypass managed classification").code(),
                    tonic::Code::PermissionDenied
                );
            } else {
                let response = result.expect("empty ordinary batch").into_inner();
                assert!(response.success && response.error.is_none());
            }
        }
        let fixture = legacy_path_fixture(&disk, "path-ordinary", LegacyPathCase::Residual(true)).await;
        let before = path_delete_snapshot(&fixture.physical);
        let response = authenticated_legacy_delete(
            &service,
            signed_legacy_delete_request(&disk_ref, &fixture.volume, &fixture.path, &fixture.fi, &fixture.options, false, true),
        )
        .await
        .expect("force/undo envelope")
        .into_inner();
        assert!(!response.success);
        let error = response.error.expect("force/undo error");
        assert_eq!(error.code, DiskError::other("undo_write cannot force a delete marker").to_u32());
        assert_eq!(error.error_info, "io error undo_write cannot force a delete marker");
        assert_eq!(path_delete_snapshot(&fixture.physical), before);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_path_delete_receiver_rejects_missing_live_volume() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-path-delete-secret".to_owned());
        let root = tempfile::tempdir().expect("missing path receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let target = service.local_mutation_target();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("receiver disk");
        for (index, case) in LegacyPathCase::ALL.into_iter().enumerate() {
            for msgpack in [false, true] {
                let volume = format!("path-missing-{index}-{msgpack}");
                if msgpack {
                    env.ecstore
                        .clone()
                        .make_artifact_bucket(&volume, "outputs/")
                        .await
                        .expect("managed authority retained after volume deletion");
                } else {
                    env.make_bucket(&volume, true).await;
                }
                let mut fixture = legacy_path_fixture(&disk, &volume, case).await;
                if msgpack {
                    fixture.path.insert(0, '/');
                }
                let incarnation = env.ecstore.bucket_incarnation_id(&volume).await.expect("authority");
                ecstore_disk::DiskAPI::delete_volume(disk.as_ref(), &volume, true)
                    .await
                    .expect("remove physical volume");
                assert_eq!(env.ecstore.bucket_incarnation_id(&volume).await.expect("retained authority"), incarnation);
                assert_eq!(
                    target.reject_legacy_artifact_mutation(&volume).await.is_err(),
                    msgpack,
                    "authority classification survives physical deletion"
                );
                assert_eq!(
                    authenticated_path_mutation(&service, &disk_ref, &fixture, msgpack, false)
                        .await
                        .expect("typed missing-volume envelope")
                        .expect_err("live volume admission fails"),
                    DiskError::VolumeNotFound
                );
                assert!(!env.disk_paths[0].join(&volume).exists(), "no missing volume recreation");
            }
        }
        env.make_bucket("path-contract", true).await;
        let mut fixtures = Vec::new();
        for case in LegacyPathCase::ALL {
            let fixture = legacy_path_fixture(&disk, "path-contract", case).await;
            let before = path_delete_snapshot(&fixture.physical);
            for unknown in ["not-a-disk-reference".to_owned(), Uuid::new_v4().to_string()] {
                let error = authenticated_path_mutation(&service, &unknown, &fixture, false, false)
                    .await
                    .expect("candidate failure envelope")
                    .expect_err("unknown disk rejects");
                // The legacy wire stores Display text; decoding an Io error wraps that text again.
                let expected = if matches!(case, LegacyPathCase::Residual(true)) {
                    DiskError::DiskNotFound
                } else {
                    DiskError::other("io error cannot find disk")
                };
                assert_eq!(error.to_string(), expected.to_string(), "exact lookup-stage error text");
                assert_eq!(error, expected, "old lookup-stage error contract");
                assert_eq!(path_delete_snapshot(&fixture.physical), before);
            }
            fixtures.push(fixture);
        }
        let id = ecstore_disk::DiskAPI::get_disk_id(disk.as_ref())
            .await
            .expect("disk identity")
            .expect("formatted disk")
            .to_string();
        target
            .find_mutation_disk(&id)
            .await
            .expect("prime instance UUID-to-path cache");
        let format = env.disk_paths[0].join(".rustfs.sys/format.json");
        let saved_format = tokio::fs::read(&format).await.expect("original format bytes");
        tokio::fs::remove_file(&format).await.expect("remove cached disk format");
        // The format cache deliberately trusts wall-clock checks for one second.
        // This wait expires that cache; it is not evidence of a physical worker drain.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        for (corrupt, typed) in [(false, DiskError::UnformattedDisk), (true, DiskError::CorruptedBackend)] {
            if corrupt {
                tokio::fs::write(&format, b"invalid format").await.expect("corrupt format");
            }
            for fixture in &fixtures {
                let before = path_delete_snapshot(&fixture.physical);
                let error = authenticated_path_mutation(&service, &id, fixture, false, false)
                    .await
                    .expect("cached identity error envelope")
                    .expect_err("format error rejects admission");
                let expected = if matches!(fixture.case, LegacyPathCase::Residual(true)) {
                    typed.clone()
                } else {
                    DiskError::other("io error cannot find disk")
                };
                assert_eq!(error.to_string(), expected.to_string(), "exact cached-ID error text");
                assert_eq!(error, expected, "cached-ID failures keep their original envelope");
                assert_eq!(path_delete_snapshot(&fixture.physical), before);
            }
        }
        tokio::fs::write(&format, saved_format).await.expect("restore real format");
        ecstore_disk::DiskAPI::get_disk_id(disk.as_ref())
            .await
            .expect("restored disk identity");
        for mut fixture in fixtures {
            let expected = if matches!(fixture.case, LegacyPathCase::Residual(_)) {
                let metadata = fixture.physical.join("xl.meta");
                let mut bytes = tokio::fs::read(&metadata).await.expect("valid metadata before bitrot");
                // Match the filemeta CRC regression: mutate the blob after its header and bin32 prefix.
                bytes[8 + 5] ^= 0xff;
                tokio::fs::write(metadata, bytes).await.expect("corrupt consume input");
                DiskError::FileCorrupt
            } else {
                fixture.path = "../outside".to_owned();
                DiskError::InvalidPath
            };
            let before = path_delete_snapshot(&fixture.physical);
            let error = authenticated_path_mutation(&service, &disk_ref, &fixture, false, false)
                .await
                .expect("consume error envelope")
                .expect_err("actual consume failure remains typed");
            assert_eq!(error, expected);
            assert_eq!(path_delete_snapshot(&fixture.physical), before, "failed consume is zero-write");
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_path_delete_receiver_binds_instance_bootstrap_and_scanner_guard() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-path-delete-secret".to_owned());
        let root = tempfile::tempdir().expect("instance receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("path-instance", true).await;
        let ready = service_for(&env).await;
        let instance = crate::storage::storage_api::bootstrap_instance_ctx();
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = ready
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("receiver disk");
        let slot = super::super::ServerContextSlot::with_instance_context(instance.clone());
        let bootstrap = super::super::make_server_for_slot(slot.clone());
        let unbound = super::super::make_server_for_slot(super::super::ServerContextSlot::new());
        let mut wrong = super::super::make_server_for_context(ready.context.clone());
        wrong.server_ctx = Some(super::super::ServerContextSlot::with_instance_context(
            crate::storage::storage_api::new_instance_ctx(),
        ));
        for case in LegacyPathCase::ALL {
            for volume in ["path-instance", ".rustfs.sys"] {
                let fixture = legacy_path_fixture(&disk, volume, case).await;
                let before = path_delete_snapshot(&fixture.physical);
                for (service, is_bootstrap) in [(&unbound, false), (&wrong, true)] {
                    let error = authenticated_path_mutation(service, &disk_ref, &fixture, false, false)
                        .await
                        .expect("unbound or bootstrap envelope")
                        .expect_err("wrong instance cannot delete");
                    let expected = if is_bootstrap && volume == "path-instance" {
                        DiskError::FileAccessDenied
                    } else if matches!(case, LegacyPathCase::Residual(true)) {
                        if is_bootstrap {
                            DiskError::DiskNotFound
                        } else {
                            DiskError::other("io error target disk instance is unavailable")
                        }
                    } else {
                        DiskError::other("io error cannot find disk")
                    };
                    assert_eq!(error.to_string(), expected.to_string(), "exact instance admission error text");
                    assert_eq!(error, expected, "instance admission keeps its original envelope");
                    assert_eq!(path_delete_snapshot(&fixture.physical), before);
                }
                let result = authenticated_path_mutation(&bootstrap, &disk_ref, &fixture, false, false)
                    .await
                    .expect("bootstrap envelope");
                if volume == ".rustfs.sys" {
                    result.expect("bootstrap internal deletion");
                    assert_path_mutation_result(&disk, &fixture).await;
                } else {
                    assert_eq!(result.expect_err("bootstrap ordinary admission rejects"), DiskError::FileAccessDenied);
                    assert_eq!(path_delete_snapshot(&fixture.physical), before);
                }
            }
        }
        let captured = bootstrap.local_mutation_target();
        slot.try_install(ready.context.clone().expect("ready context"))
            .expect("publish ready slot");
        assert_eq!(
            captured
                .begin_legacy_local_path_delete(&disk_ref, "path-instance", LegacyPathDeleteOperation::Delete, None)
                .await
                .err()
                .expect("captured bootstrap cannot upgrade"),
            DiskError::FileAccessDenied
        );

        let mut fixture = legacy_path_fixture(&disk, "path-instance", LegacyPathCase::Delete).await;
        let ttl = std::time::Duration::from_millis(crate::storage::storage_api::SCANNER_PUBLICATION_LEASE_TTL_MS);
        let (scanner, _) = env
            .ecstore
            .acquire_scanner_publication_lease(env.ecstore.scanner_data_movement_generation(), ttl)
            .await
            .expect("real scanner token");
        fixture.scanner = Some(scanner);
        let before = path_delete_snapshot(&fixture.physical);
        let mut unsigned = signed_path_delete_request(&disk_ref, &fixture);
        unsigned.metadata_mut().remove("x-rustfs-content-sha256");
        assert_eq!(
            ready
                .handle_delete(unsigned)
                .await
                .expect_err("scanner requires body digest")
                .code(),
            tonic::Code::PermissionDenied
        );
        let mut combined = signed_path_delete_request(&disk_ref, &fixture).into_inner();
        combined.bucket_incarnation_id = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
        let mut combined = Request::new(combined);
        let body = rustfs_protos::canonical_delete_request_body(combined.get_ref()).expect("combined body");
        sign_legacy_rpc_request(&mut combined, &body, "Delete");
        verify_legacy_rpc_request(&combined, "Delete").expect("signed combined request");
        assert_eq!(
            ready
                .handle_delete(combined)
                .await
                .expect_err("incarnation and scanner are exclusive")
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(path_delete_snapshot(&fixture.physical), before);
        for bytes in [Bytes::from_static(b"bad"), Bytes::copy_from_slice(Uuid::nil().as_bytes())] {
            let mut request = signed_path_delete_request(&disk_ref, &fixture).into_inner();
            request.scanner_publication_lease_token = bytes;
            let mut request = Request::new(request);
            let body = rustfs_protos::canonical_delete_request_body(request.get_ref()).expect("invalid token body");
            sign_legacy_rpc_request(&mut request, &body, "Delete");
            verify_legacy_rpc_request(&request, "Delete").expect("signed invalid token");
            assert_eq!(
                ready.handle_delete(request).await.expect_err("invalid scanner UUID").code(),
                tonic::Code::InvalidArgument
            );
            assert_eq!(path_delete_snapshot(&fixture.physical), before);
        }
        let bootstrap_only =
            super::super::make_server_for_slot(super::super::ServerContextSlot::with_instance_context(instance.clone()));
        authenticated_path_mutation(&bootstrap_only, &disk_ref, &fixture, false, false)
            .await
            .expect("bootstrap scanner envelope")
            .expect_err("bootstrap has no Ready scanner authority");
        assert_eq!(path_delete_snapshot(&fixture.physical), before);
        authenticated_path_mutation(&ready, &disk_ref, &fixture, false, true)
            .await
            .expect_err("scanner body is bound");
        assert_eq!(path_delete_snapshot(&fixture.physical), before);
        authenticated_path_mutation(&ready, &disk_ref, &fixture, false, false)
            .await
            .expect("scanner envelope")
            .expect("captured Ready scanner delete");
        assert_path_mutation_result(&disk, &fixture).await;
        assert!(env.ecstore.release_scanner_publication_lease(scanner).await);

        let other_root = tempfile::tempdir().expect("foreign scanner instance");
        let other = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(other_root.path())
            .instance_ctx(crate::storage::storage_api::new_instance_ctx())
            .init_bucket_metadata(false)
            .build()
            .await;
        let (foreign, _) = other
            .ecstore
            .acquire_scanner_publication_lease(other.ecstore.scanner_data_movement_generation(), ttl)
            .await
            .expect("foreign token");
        let mut fixture = legacy_path_fixture(&disk, "path-instance", LegacyPathCase::Delete).await;
        fixture.scanner = Some(foreign);
        let before = path_delete_snapshot(&fixture.physical);
        authenticated_path_mutation(&ready, &disk_ref, &fixture, false, false)
            .await
            .expect("foreign scanner envelope")
            .expect_err("cross-instance token rejects");
        assert_eq!(path_delete_snapshot(&fixture.physical), before);
        assert!(other.ecstore.release_scanner_publication_lease(foreign).await);

        let (scanner, _) = env
            .ecstore
            .acquire_scanner_publication_lease(env.ecstore.scanner_data_movement_generation(), ttl)
            .await
            .expect("new scanner token");
        let guard = Arc::new(
            env.ecstore
                .acquire_scanner_publication_lease_guard(scanner)
                .await
                .expect("captured Ready guard"),
        );
        let weak_guard = Arc::downgrade(&guard);
        let admitted = ready
            .local_mutation_target()
            .begin_legacy_local_path_delete(&disk_ref, &fixture.volume, LegacyPathDeleteOperation::Delete, Some(guard))
            .await
            .expect("admitted original disk with scanner guard");
        assert!(env.ecstore.release_scanner_publication_lease(scanner).await);
        assert!(weak_guard.upgrade().is_some(), "removing token does not release admitted guard");
        crate::storage::storage_api::init_local_disks_with_instance_ctx(&instance, env.endpoint_pools.clone())
            .await
            .expect("replace registry handles");
        let replacement = ready
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("replacement disk");
        assert!(!Arc::ptr_eq(&disk, &replacement));
        let info_options = DiskInfoOptions {
            metrics: true,
            noop: true,
            ..Default::default()
        };
        let original_before = ecstore_disk::DiskAPI::disk_info(disk.as_ref(), &info_options)
            .await
            .expect("original metrics")
            .metrics;
        let replacement_before = ecstore_disk::DiskAPI::disk_info(replacement.as_ref(), &info_options)
            .await
            .expect("replacement metrics")
            .metrics;
        admitted
            .delete(&fixture.path, fixture.options.clone())
            .await
            .expect("captured original token consumes");
        assert_path_mutation_result(&disk, &fixture).await;
        assert!(weak_guard.upgrade().is_none(), "consuming the admitted token releases its scanner guard");
        let original_after = ecstore_disk::DiskAPI::disk_info(disk.as_ref(), &info_options)
            .await
            .expect("original metrics after")
            .metrics;
        let replacement_after = ecstore_disk::DiskAPI::disk_info(replacement.as_ref(), &info_options)
            .await
            .expect("replacement metrics after")
            .metrics;
        assert_eq!(original_after.total_deletes, original_before.total_deletes + 1);
        assert_eq!(replacement_after.total_deletes, replacement_before.total_deletes);
        for case in [
            LegacyPathCase::DeletePaths,
            LegacyPathCase::Residual(false),
            LegacyPathCase::Residual(true),
        ] {
            let target = ready.local_mutation_target();
            let original = target.find_mutation_disk(&disk_ref).await.expect("current disk");
            let fixture = legacy_path_fixture(&original, "path-instance", case).await;
            let (path_delete, version_delete) = match case {
                LegacyPathCase::DeletePaths => (
                    Some(
                        target
                            .begin_legacy_local_path_delete(
                                &disk_ref,
                                &fixture.volume,
                                LegacyPathDeleteOperation::DeletePaths,
                                None,
                            )
                            .await
                            .expect("captured batch"),
                    ),
                    None,
                ),
                LegacyPathCase::Residual(undo_write) => (
                    None,
                    Some(
                        target
                            .begin_legacy_local_residual_version_delete(&disk_ref, &fixture.volume, &fixture.path, undo_write)
                            .await
                            .expect("captured residual"),
                    ),
                ),
                _ => unreachable!(),
            };
            crate::storage::storage_api::init_local_disks_with_instance_ctx(&instance, env.endpoint_pools.clone())
                .await
                .expect("replace admitted registry");
            let replacement = ready
                .local_mutation_target()
                .find_mutation_disk(&disk_ref)
                .await
                .expect("new registry disk");
            assert!(!Arc::ptr_eq(&original, &replacement));
            let before = ecstore_disk::DiskAPI::disk_info(original.as_ref(), &info_options)
                .await
                .expect("captured metrics")
                .metrics;
            let other_before = ecstore_disk::DiskAPI::disk_info(replacement.as_ref(), &info_options)
                .await
                .expect("new metrics")
                .metrics;
            if let Some(delete) = path_delete {
                delete
                    .delete_paths(&[fixture.path.clone(), fixture.path.clone()])
                    .await
                    .expect("captured batch consumes");
            }
            if let Some(delete) = version_delete {
                delete
                    .delete_version_residual(fixture.fi.clone(), false, fixture.options.clone())
                    .await
                    .expect("captured residual consumes");
            }
            assert_path_mutation_result(&original, &fixture).await;
            let after = ecstore_disk::DiskAPI::disk_info(original.as_ref(), &info_options)
                .await
                .expect("captured metrics after")
                .metrics;
            let other_after = ecstore_disk::DiskAPI::disk_info(replacement.as_ref(), &info_options)
                .await
                .expect("new metrics after")
                .metrics;
            assert_eq!(after.total_deletes, before.total_deletes + 1);
            assert_eq!(other_after.total_deletes, other_before.total_deletes);
        }
    }

    async fn remote_path_mutation(remote: &ecstore_disk::DiskStore, fixture: &LegacyPathFixture) -> Result<(), DiskError> {
        match fixture.case {
            LegacyPathCase::Delete => {
                ecstore_disk::DiskAPI::delete(remote.as_ref(), &fixture.volume, &fixture.path, fixture.options.clone()).await
            }
            LegacyPathCase::DeletePaths => {
                ecstore_disk::DiskAPI::delete_paths(
                    remote.as_ref(),
                    &fixture.volume,
                    &[fixture.path.clone(), fixture.path.clone()],
                )
                .await
            }
            LegacyPathCase::Residual(_) => {
                ecstore_disk::DiskAPI::delete_version(
                    remote.as_ref(),
                    &fixture.volume,
                    &fixture.path,
                    fixture.fi.clone(),
                    false,
                    fixture.options.clone(),
                )
                .await
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_path_delete_remote_delete_batch_and_residual() {
        use crate::storage::storage_api::{
            Endpoint, EndpointServerPools, Endpoints,
            ecstore_disk::{DiskOption, new_disk},
            init_local_disks_with_instance_ctx,
        };
        use rustfs_protos::proto_gen::node_service::node_service_server::NodeServiceServer;
        use tokio_stream::wrappers::TcpListenerStream;
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-path-delete-secret".to_owned());
        let root = tempfile::tempdir().expect("remote path deletion root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("path-remote", true).await;
        let ready = service_for(&env).await;
        let instance = crate::storage::storage_api::bootstrap_instance_ctx();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("loopback listener");
        let addr = listener.local_addr().expect("loopback address");
        let mut endpoints = env.endpoint_pools.as_ref()[0].endpoints.as_ref().clone();
        let mut endpoint =
            Endpoint::try_from(format!("http://{addr}{}", env.disk_paths[0].display()).as_str()).expect("remote endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        endpoint.is_local = true;
        endpoints[0] = endpoint.clone();
        let mut pool = env.endpoint_pools.as_ref()[0].clone();
        pool.endpoints = Endpoints::from(endpoints);
        init_local_disks_with_instance_ctx(&instance, EndpointServerPools::from(vec![pool]))
            .await
            .expect("register receiving disk");
        let service = super::super::make_server_for_context(ready.context.clone());
        let local = service
            .local_mutation_target()
            .find_mutation_disk(&endpoint.to_string())
            .await
            .expect("local receiving disk");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        let audience = addr.to_string();
        let auth = tower::ServiceBuilder::new().map_request(move |request: http::Request<_>| {
            let path = request.uri().path();
            crate::storage::storage_api::ecstore_rpc::verify_tonic_rpc_signature_with_bootstrap(
                &audience,
                path,
                request.headers(),
                path == "/node_service.NodeService/Ping",
            )
            .expect("real remote HMAC signature");
            if matches!(
                path,
                "/node_service.NodeService/Delete"
                    | "/node_service.NodeService/DeletePaths"
                    | "/node_service.NodeService/DeleteVersion"
            ) {
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            request
        });
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .layer(auth)
                .add_service(NodeServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                })
                .await
                .expect("path delete server");
        });
        endpoint.is_local = false;
        let remote = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("real remote client");
        assert!(!ecstore_disk::DiskAPI::is_local(remote.as_ref()));
        for case in LegacyPathCase::ALL {
            let mut fixture = legacy_path_fixture(&local, "path-remote", case).await;
            if !matches!(case, LegacyPathCase::Residual(_)) {
                fixture.path.insert(0, '/');
            }
            remote_path_mutation(&remote, &fixture).await.expect("real network deletion");
            assert_path_mutation_result(&local, &fixture).await;
        }
        for undo_write in [false, true] {
            let fixture = legacy_residual_file_fixture(&local, "path-remote", undo_write).await;
            remote_path_mutation(&remote, &fixture)
                .await
                .expect("network slash residual deletes regular file");
            assert_path_mutation_result(&local, &fixture).await;
        }
        env.make_bucket("path-retry", true).await;
        let mut fixtures = Vec::new();
        for case in LegacyPathCase::ALL {
            fixtures.push(legacy_path_fixture(&local, "path-retry", case).await);
        }
        let incarnation = env
            .ecstore
            .bucket_incarnation_id("path-retry")
            .await
            .expect("retry authority");
        ecstore_disk::DiskAPI::delete_volume(local.as_ref(), "path-retry", true)
            .await
            .expect("remove receiving physical volume");
        assert_eq!(
            env.ecstore
                .bucket_incarnation_id("path-retry")
                .await
                .expect("authority retained"),
            incarnation
        );
        for fixture in &fixtures {
            assert_eq!(
                remote_path_mutation(&remote, fixture)
                    .await
                    .expect_err("network preserves missing live volume"),
                DiskError::VolumeNotFound
            );
            assert!(!env.disk_paths[0].join("path-retry").exists());
        }
        ecstore_disk::DiskAPI::make_volume(local.as_ref(), "path-retry")
            .await
            .expect("restore physical volume");
        for fixture in &fixtures {
            if let Some(previous) = &fixture.previous {
                ecstore_disk::DiskAPI::write_metadata(local.as_ref(), "", &fixture.volume, &fixture.path, previous.clone())
                    .await
                    .expect("restore history");
                ecstore_disk::DiskAPI::write_metadata(local.as_ref(), "", &fixture.volume, &fixture.path, fixture.fi.clone())
                    .await
                    .expect("restore selected version");
            } else {
                tokio::fs::create_dir_all(&fixture.physical)
                    .await
                    .expect("restore retry path");
                tokio::fs::write(fixture.physical.join("payload"), LEGACY_RENAME_BODY)
                    .await
                    .expect("restore retry bytes");
            }
            remote_path_mutation(&remote, fixture)
                .await
                .expect("same network request succeeds after volume recovery");
            assert_path_mutation_result(&local, fixture).await;
        }
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            14,
            "three RPC methods carry ten successes and four typed failures"
        );
        ecstore_disk::DiskAPI::close(remote.as_ref())
            .await
            .expect("close remote client");
        drop(remote);
        shutdown.send(()).expect("stop server");
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("server drains")
            .expect("server task");
    }

    #[derive(Clone, Copy, Debug)]
    enum LegacyPartStage {
        Prepare,
        Publish,
        Commit,
        Rollback,
    }

    impl LegacyPartStage {
        const ALL: [Self; 4] = [Self::Prepare, Self::Publish, Self::Commit, Self::Rollback];

        fn action(self) -> PartTransactionAction {
            if matches!(self, Self::Rollback) {
                PartTransactionAction::Rollback
            } else {
                PartTransactionAction::Commit
            }
        }

        fn operation(self) -> LegacyPartOperation {
            match self {
                Self::Prepare => LegacyPartOperation::Prepare,
                Self::Publish => LegacyPartOperation::Rename,
                Self::Commit | Self::Rollback => LegacyPartOperation::Settle,
            }
        }

        fn operation_name(self) -> &'static str {
            match self {
                Self::Prepare => "prepare_part_transaction",
                Self::Publish => "rename_part",
                Self::Commit | Self::Rollback => "settle_part_transaction",
            }
        }

        fn has_source(self) -> bool {
            matches!(self, Self::Prepare | Self::Publish)
        }
    }

    struct LegacyPartFixture {
        source: (String, String),
        destination: (String, String),
        src: std::path::PathBuf,
        dst: std::path::PathBuf,
        metadata: [Bytes; 2],
        indices: [(String, Bytes); 2],
    }

    impl LegacyPartFixture {
        fn source(&self) -> (&str, &str) {
            (&self.source.0, &self.source.1)
        }
        fn destination(&self) -> (&str, &str) {
            (&self.destination.0, &self.destination.1)
        }
        fn transaction(&self) -> std::path::PathBuf {
            self.dst.parent().expect("destination parent").join(".part.1.rustfs-txn")
        }
    }

    async fn legacy_part_fixture(disk: &ecstore_disk::DiskStore, src_volume: &str, dst_volume: &str) -> LegacyPartFixture {
        use rustfs_filemeta::shard_integrity::{IntegrityLayout, PartIntegrity};
        let prefix = format!("outputs/74656e616e74/{}/{}", Uuid::new_v4(), Uuid::new_v4());
        let source = (src_volume.to_owned(), format!("{prefix}/source/part.1"));
        let destination = (dst_volume.to_owned(), format!("{prefix}/destination/part.1"));
        let root = ecstore_disk::DiskAPI::path(disk.as_ref());
        let src = root.join(&source.0).join(&source.1);
        let dst = root.join(&destination.0).join(&destination.1);
        let mut metadata = Vec::new();
        let mut indices = Vec::new();
        for (path, byte) in [(&dst, b'a'), (&src, b'b')] {
            let mut integrity =
                PartIntegrity::new(IntegrityLayout::new(2, 2, 8, false).expect("layout"), 1).expect("part identity");
            integrity.size = 8;
            integrity.stripes = 1;
            let payload = [byte; 4];
            let mut record = Vec::new();
            for index in 0..4 {
                record.extend_from_slice(&integrity.shard_digest(0, index, &payload).expect("shard digest"));
            }
            integrity.root = integrity.root_digest(&integrity.leaf_digest(0, &record).expect("leaf"));
            integrity.validate().expect("valid integrity metadata");
            integrity
                .verify_shard(0, 0, &payload, &record)
                .expect("valid proof and shard");
            let mut bytes = integrity.index_header().to_vec();
            bytes.extend_from_slice(&record);
            assert_eq!(bytes.len(), integrity.index_size().expect("index size"));
            let name = integrity.file_name();
            let meta = Bytes::from(
                rustfs_filemeta::ObjectPartInfo {
                    number: 1,
                    size: 8,
                    integrity: Some(integrity),
                    ..Default::default()
                }
                .marshal_msg()
                .expect("part metadata"),
            );
            tokio::fs::create_dir_all(path.parent().expect("part parent"))
                .await
                .expect("part directory");
            tokio::fs::write(path, payload).await.expect("part bytes");
            tokio::fs::write(path.with_extension("1.meta"), &meta)
                .await
                .expect("part metadata bytes");
            tokio::fs::write(path.parent().expect("part parent").join(&name), &bytes)
                .await
                .expect("proof bytes");
            metadata.push(meta);
            indices.push((name, Bytes::from(bytes)));
        }
        LegacyPartFixture {
            source,
            destination,
            src,
            dst,
            metadata: metadata.try_into().expect("two metadata generations"),
            indices: indices.try_into().expect("two proof generations"),
        }
    }

    fn legacy_part_snapshot(fixture: &LegacyPartFixture) -> Vec<(std::path::PathBuf, Option<Vec<u8>>)> {
        let mut pending = vec![
            fixture.src.parent().expect("source parent").to_path_buf(),
            fixture.dst.parent().expect("destination parent").to_path_buf(),
        ];
        let mut snapshot = Vec::new();
        while let Some(path) = pending.pop() {
            if !path.exists() {
                continue;
            }
            if path.is_dir() {
                snapshot.push((path.clone(), None));
                pending.extend(
                    std::fs::read_dir(path)
                        .expect("fixture directory")
                        .map(|entry| entry.expect("fixture entry").path()),
                );
            } else {
                snapshot.push((path.clone(), Some(std::fs::read(path).expect("fixture bytes"))));
            }
        }
        snapshot.sort();
        snapshot
    }

    async fn prime_legacy_part(disk: &ecstore_disk::DiskStore, fixture: &LegacyPartFixture, stage: LegacyPartStage) {
        let (sv, sp) = fixture.source();
        let (dv, dp) = fixture.destination();
        if !matches!(stage, LegacyPartStage::Prepare) {
            ecstore_disk::DiskAPI::prepare_part_transaction(disk.as_ref(), sv, sp, dv, dp, fixture.metadata[1].clone())
                .await
                .expect("prime real transaction");
        }
        if !stage.has_source() {
            ecstore_disk::DiskAPI::rename_part(disk.as_ref(), sv, sp, dv, dp, fixture.metadata[1].clone())
                .await
                .expect("prime real publication");
        }
    }

    async fn assert_legacy_part_settled(fixture: &LegacyPartFixture, action: PartTransactionAction) {
        let retained = usize::from(action == PartTransactionAction::Commit);
        assert_eq!(
            tokio::fs::read(&fixture.dst).await.expect("settled data"),
            if retained == 1 { b"bbbb" } else { b"aaaa" }
        );
        assert_eq!(
            tokio::fs::read(fixture.dst.with_extension("1.meta"))
                .await
                .expect("settled metadata"),
            fixture.metadata[retained]
        );
        for (index, (name, bytes)) in fixture.indices.iter().enumerate() {
            let path = fixture.dst.parent().expect("destination parent").join(name);
            if index == retained {
                assert_eq!(tokio::fs::read(path).await.expect("retained proof"), *bytes);
            } else {
                assert!(!path.exists(), "obsolete index removed");
            }
        }
        assert!(!fixture.transaction().exists(), "settlement consumes transaction");
    }

    async fn authenticated_legacy_part(
        service: &NodeService,
        disk: &str,
        fixture: &LegacyPartFixture,
        stage: LegacyPartStage,
        tamper: bool,
    ) -> Result<Result<(), DiskError>, Status> {
        let (success, error) = match stage {
            LegacyPartStage::Prepare => {
                let mut request = Request::new(PreparePartTransactionRequest {
                    disk: disk.to_owned(),
                    src_volume: fixture.source.0.clone(),
                    src_path: fixture.source.1.clone(),
                    dst_volume: fixture.destination.0.clone(),
                    dst_path: fixture.destination.1.clone(),
                    meta: fixture.metadata[1].clone(),
                });
                let body =
                    rustfs_protos::canonical_prepare_part_transaction_request_body(request.get_ref()).expect("canonical prepare");
                sign_legacy_rpc_request(&mut request, &body, "PreparePartTransaction");
                if tamper {
                    request.get_mut().meta = Bytes::from_static(b"tampered");
                }
                verify_legacy_rpc_request(&request, "PreparePartTransaction")?;
                let response = service.handle_prepare_part_transaction(request).await?.into_inner();
                (response.success, response.error)
            }
            LegacyPartStage::Publish => {
                let mut request = Request::new(RenamePartRequest {
                    disk: disk.to_owned(),
                    src_volume: fixture.source.0.clone(),
                    src_path: fixture.source.1.clone(),
                    dst_volume: fixture.destination.0.clone(),
                    dst_path: fixture.destination.1.clone(),
                    meta: fixture.metadata[1].clone(),
                });
                let body = rustfs_protos::canonical_rename_part_request_body(request.get_ref()).expect("canonical rename part");
                sign_legacy_rpc_request(&mut request, &body, "RenamePart");
                if tamper {
                    request.get_mut().meta = Bytes::from_static(b"tampered");
                }
                verify_legacy_rpc_request(&request, "RenamePart")?;
                let response = service.handle_rename_part(request).await?.into_inner();
                (response.success, response.error)
            }
            LegacyPartStage::Commit | LegacyPartStage::Rollback => {
                let mut request = Request::new(SettlePartTransactionRequest {
                    disk: disk.to_owned(),
                    volume: fixture.destination.0.clone(),
                    path: fixture.destination.1.clone(),
                    rollback: matches!(stage, LegacyPartStage::Rollback),
                });
                let body = rustfs_protos::canonical_settle_part_transaction_request_body(request.get_ref())
                    .expect("canonical settlement");
                sign_legacy_rpc_request(&mut request, &body, "SettlePartTransaction");
                if tamper {
                    request.get_mut().rollback = !matches!(stage, LegacyPartStage::Rollback);
                }
                verify_legacy_rpc_request(&request, "SettlePartTransaction")?;
                let response = service.handle_settle_part_transaction(request).await?.into_inner();
                (response.success, response.error)
            }
        };
        if success {
            assert!(error.is_none());
            Ok(Ok(()))
        } else {
            Ok(Err(error.expect("failed typed envelope").into()))
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_part_mutation_receiver_preserves_signed_stages_and_managed_rejection() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-part-receiver-secret".to_owned());
        let root = tempfile::tempdir().expect("receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("part-ordinary", true).await;
        env.ecstore
            .clone()
            .make_artifact_bucket("part-managed", "outputs/")
            .await
            .expect("managed bucket");
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = service
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("receiving disk");
        for stage in LegacyPartStage::ALL {
            for (source, destination) in [
                ("part-ordinary", "part-ordinary"),
                (".rustfs.sys/tmp", ".rustfs.sys"),
                ("part-managed", "part-ordinary"),
                ("part-ordinary", "part-managed"),
            ] {
                let fixture = legacy_part_fixture(&disk, source, destination).await;
                prime_legacy_part(&disk, &fixture, stage).await;
                let before = legacy_part_snapshot(&fixture);
                assert_eq!(
                    authenticated_legacy_part(&service, &disk_ref, &fixture, stage, true)
                        .await
                        .expect_err("stage body is signed")
                        .code(),
                    tonic::Code::PermissionDenied
                );
                assert_eq!(legacy_part_snapshot(&fixture), before);
                let result = authenticated_legacy_part(&service, &disk_ref, &fixture, stage, false).await;
                let managed = destination == "part-managed" || (stage.has_source() && source == "part-managed");
                if managed {
                    assert_eq!(
                        result.expect_err("every affected managed volume rejects").code(),
                        tonic::Code::PermissionDenied
                    );
                    assert_eq!(legacy_part_snapshot(&fixture), before);
                } else {
                    result.expect("signed envelope").expect("stage succeeds");
                    if !stage.has_source() {
                        assert_legacy_part_settled(&fixture, stage.action()).await;
                    } else if matches!(stage, LegacyPartStage::Prepare) {
                        assert!(fixture.transaction().is_dir());
                        assert_eq!(tokio::fs::read(&fixture.dst).await.expect("prepare keeps old data"), b"aaaa");
                    } else {
                        assert!(!fixture.src.exists());
                        assert_eq!(tokio::fs::read(&fixture.dst).await.expect("published data"), b"bbbb");
                    }
                }
            }
            let fixture = legacy_part_fixture(&disk, "part-ordinary", "part-ordinary").await;
            let before = legacy_part_snapshot(&fixture);
            let error = authenticated_legacy_part(&service, "missing-disk", &fixture, stage, false)
                .await
                .expect("missing disk envelope")
                .expect_err("missing disk");
            assert!(error.to_string().contains("cannot find disk"));
            assert_eq!(legacy_part_snapshot(&fixture), before);
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_part_mutation_receiver_rejects_missing_live_volumes() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-part-receiver-secret".to_owned());
        let root = tempfile::tempdir().expect("missing-volume root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let service = service_for(&env).await;
        let target = service.local_mutation_target();
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("receiving disk");
        for stage in LegacyPartStage::ALL {
            for missing_source in [false, true] {
                if missing_source && !stage.has_source() {
                    continue;
                }
                let src_volume = format!("part-src-{}", Uuid::new_v4());
                let dst_volume = format!("part-dst-{}", Uuid::new_v4());
                env.make_bucket(&src_volume, true).await;
                env.make_bucket(&dst_volume, true).await;
                let fixture = legacy_part_fixture(&disk, &src_volume, &dst_volume).await;
                prime_legacy_part(&disk, &fixture, stage).await;
                let missing = if missing_source { &src_volume } else { &dst_volume };
                let incarnation = env.ecstore.bucket_incarnation_id(missing).await.expect("incarnation");
                disk.delete_volume(missing, true).await.expect("remove physical volume");
                assert_eq!(env.ecstore.bucket_incarnation_id(missing).await.expect("retained authority"), incarnation);
                target
                    .reject_legacy_artifact_mutation(missing)
                    .await
                    .expect("authority still ordinary");
                let before = legacy_part_snapshot(&fixture);
                let metrics_before = ecstore_disk::DiskAPI::disk_info(
                    disk.as_ref(),
                    &DiskInfoOptions {
                        metrics: true,
                        noop: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("metrics before admission failure")
                .metrics;
                assert_eq!(
                    authenticated_legacy_part(&service, &disk_ref, &fixture, stage, false)
                        .await
                        .expect("typed failure")
                        .expect_err("missing live volume"),
                    DiskError::VolumeNotFound
                );
                assert_eq!(legacy_part_snapshot(&fixture), before);
                assert!(!env.disk_paths[0].join(missing).exists(), "never recreate missing volume");
                let metrics_after = ecstore_disk::DiskAPI::disk_info(
                    disk.as_ref(),
                    &DiskInfoOptions {
                        metrics: true,
                        noop: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("metrics after admission failure")
                .metrics;
                let op = stage.operation_name();
                assert_eq!(
                    metrics_after.api_calls.get(op).copied().unwrap_or(0),
                    metrics_before.api_calls.get(op).copied().unwrap_or(0) + 1
                );
                assert_eq!(
                    metrics_after.last_minute.get(op).map(|action| action.count).unwrap_or(0),
                    metrics_before.last_minute.get(op).map(|action| action.count).unwrap_or(0) + 1
                );
                assert_eq!(metrics_after.total_writes, metrics_before.total_writes);
                assert_eq!(metrics_after.total_errors_timeout, metrics_before.total_errors_timeout);
                assert_eq!(metrics_after.total_errors_availability, metrics_before.total_errors_availability);
                assert_eq!(metrics_after.total_waiting, 0);
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_part_mutation_receiver_binds_instance_and_bootstrap() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-part-receiver-secret".to_owned());
        let root = tempfile::tempdir().expect("instance root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("part-instance", true).await;
        let ready = service_for(&env).await;
        let instance = crate::storage::storage_api::bootstrap_instance_ctx();
        assert!(super::super::BootstrapLocalTarget::new(instance.clone()).is_for_store(&env.ecstore));
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = ready
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("instance disk");
        let slot = super::super::ServerContextSlot::with_instance_context(instance.clone());
        let bootstrap = super::super::make_server_for_slot(slot.clone());
        let unbound = super::super::make_server_for_slot(super::super::ServerContextSlot::new());
        let mut wrong = super::super::make_server_for_context(ready.context.clone());
        wrong.server_ctx = Some(super::super::ServerContextSlot::with_instance_context(
            crate::storage::storage_api::new_instance_ctx(),
        ));
        for stage in LegacyPartStage::ALL {
            for (source, destination) in [
                (".rustfs.sys/tmp", ".rustfs.sys"),
                (".rustfs.sys/tmp", "part-instance"),
                ("part-instance", ".rustfs.sys"),
            ] {
                let fixture = legacy_part_fixture(&disk, source, destination).await;
                prime_legacy_part(&disk, &fixture, stage).await;
                let before = legacy_part_snapshot(&fixture);
                for service in [&unbound, &wrong] {
                    authenticated_legacy_part(service, &disk_ref, &fixture, stage, false)
                        .await
                        .expect("unbound envelope")
                        .expect_err("instance unavailable");
                    assert_eq!(legacy_part_snapshot(&fixture), before);
                }
                let result = authenticated_legacy_part(&bootstrap, &disk_ref, &fixture, stage, false)
                    .await
                    .expect("bootstrap envelope");
                if destination == "part-instance" || (stage.has_source() && source == "part-instance") {
                    assert_eq!(
                        result.expect_err("bootstrap only permits involved internal volumes"),
                        DiskError::FileAccessDenied
                    );
                    assert_eq!(legacy_part_snapshot(&fixture), before);
                } else {
                    result.expect("internal stage succeeds");
                }
            }
        }
        let captured = bootstrap.local_mutation_target();
        slot.try_install(ready.context.clone().expect("ready context"))
            .expect("publish ready slot");
        for stage in LegacyPartStage::ALL {
            let disk = ready
                .local_mutation_target()
                .find_mutation_disk(&disk_ref)
                .await
                .expect("current disk");
            let fixture = legacy_part_fixture(&disk, "part-instance", "part-instance").await;
            prime_legacy_part(&disk, &fixture, stage).await;
            let source = stage.has_source().then(|| fixture.source());
            assert_eq!(
                captured
                    .begin_legacy_local_part_mutation(&disk_ref, source, fixture.destination(), stage.operation())
                    .await
                    .err()
                    .expect("captured bootstrap cannot upgrade"),
                DiskError::FileAccessDenied
            );
            let admitted = ready
                .local_mutation_target()
                .begin_legacy_local_part_mutation(&disk_ref, source, fixture.destination(), stage.operation())
                .await
                .expect("captured part token");
            crate::storage::storage_api::init_local_disks_with_instance_ctx(&instance, env.endpoint_pools.clone())
                .await
                .expect("replace registry");
            let replacement = ready
                .local_mutation_target()
                .find_mutation_disk(&disk_ref)
                .await
                .expect("replacement disk");
            assert!(!Arc::ptr_eq(&disk, &replacement));
            match stage {
                LegacyPartStage::Prepare => admitted.prepare(fixture.metadata[1].clone()).await,
                LegacyPartStage::Publish => admitted.rename(fixture.metadata[1].clone()).await,
                _ => admitted.settle(stage.action()).await,
            }
            .expect("admitted token retains original instance");
            if !stage.has_source() {
                assert_legacy_part_settled(&fixture, stage.action()).await;
            } else {
                assert!(fixture.transaction().is_dir());
            }
            let blocked = ready
                .local_mutation_target()
                .begin_legacy_local_part_mutation(&disk_ref, source, fixture.destination(), stage.operation())
                .await
                .expect("hold common destination namespace");
            let before = legacy_part_snapshot(&fixture);
            let metrics_before = ecstore_disk::DiskAPI::disk_info(
                replacement.as_ref(),
                &DiskInfoOptions {
                    metrics: true,
                    noop: true,
                    ..Default::default()
                },
            )
            .await
            .expect("metrics before blocked admission")
            .metrics;
            let result = temp_env::async_with_vars([(rustfs_config::ENV_DRIVE_MAX_TIMEOUT_DURATION, Some("1"))], async {
                tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    authenticated_legacy_part(&ready, &disk_ref, &fixture, stage, false),
                )
                .await
                .expect("namespace admission has a finite deadline")
            })
            .await
            .expect("timeout typed envelope");
            assert_eq!(result.expect_err("common destination is held"), DiskError::Timeout);
            assert_eq!(legacy_part_snapshot(&fixture), before);
            drop(blocked);
            let metrics_after = ecstore_disk::DiskAPI::disk_info(
                replacement.as_ref(),
                &DiskInfoOptions {
                    metrics: true,
                    noop: true,
                    ..Default::default()
                },
            )
            .await
            .expect("metrics after blocked admission")
            .metrics;
            let op = stage.operation_name();
            assert_eq!(
                metrics_after.api_calls.get(op).copied().unwrap_or(0),
                metrics_before.api_calls.get(op).copied().unwrap_or(0) + 1
            );
            assert_eq!(
                metrics_after.last_minute.get(op).map(|action| action.count).unwrap_or(0),
                metrics_before.last_minute.get(op).map(|action| action.count).unwrap_or(0) + 1
            );
            assert_eq!(metrics_after.total_errors_timeout, metrics_before.total_errors_timeout + 1);
            assert_eq!(metrics_after.total_errors_availability, metrics_before.total_errors_availability);
            assert_eq!(metrics_after.total_writes, metrics_before.total_writes);
            assert_eq!(metrics_after.total_waiting, 0);
        }
    }
    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_part_mutation_flow_remote_prepare_publish_settle() {
        use crate::storage::storage_api::{
            Endpoint, EndpointServerPools, Endpoints,
            ecstore_disk::{DiskOption, new_disk},
            init_local_disks_with_instance_ctx,
        };
        use rustfs_protos::proto_gen::node_service::node_service_server::NodeServiceServer;
        use tokio_stream::wrappers::TcpListenerStream;
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-part-remote-secret".to_owned());
        let root = tempfile::tempdir().expect("remote part root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("part-remote", true).await;
        let ready = service_for(&env).await;
        let instance = crate::storage::storage_api::bootstrap_instance_ctx();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("loopback listener");
        let addr = listener.local_addr().expect("loopback address");
        let mut endpoints = env.endpoint_pools.as_ref()[0].endpoints.as_ref().clone();
        let mut endpoint =
            Endpoint::try_from(format!("http://{addr}{}", env.disk_paths[0].display()).as_str()).expect("remote endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        endpoint.is_local = true;
        endpoints[0] = endpoint.clone();
        let mut pool = env.endpoint_pools.as_ref()[0].clone();
        pool.endpoints = Endpoints::from(endpoints);
        init_local_disks_with_instance_ctx(&instance, EndpointServerPools::from(vec![pool]))
            .await
            .expect("register receiving disk");
        let service = super::super::make_server_for_context(ready.context.clone());
        let local = service
            .local_mutation_target()
            .find_mutation_disk(&endpoint.to_string())
            .await
            .expect("local receiving disk");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        let audience = addr.to_string();
        let auth = tower::ServiceBuilder::new().map_request(move |request: http::Request<_>| {
            let path = request.uri().path();
            crate::storage::storage_api::ecstore_rpc::verify_tonic_rpc_signature_with_bootstrap(
                &audience,
                path,
                request.headers(),
                path == "/node_service.NodeService/Ping",
            )
            .expect("real remote HMAC signature");
            if matches!(
                path,
                "/node_service.NodeService/PreparePartTransaction"
                    | "/node_service.NodeService/RenamePart"
                    | "/node_service.NodeService/SettlePartTransaction"
            ) {
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            request
        });
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .layer(auth)
                .add_service(NodeServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                })
                .await
                .expect("part server");
        });
        endpoint.is_local = false;
        let remote = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("real remote client");
        assert!(!ecstore_disk::DiskAPI::is_local(remote.as_ref()));
        for action in [PartTransactionAction::Commit, PartTransactionAction::Rollback] {
            let fixture = legacy_part_fixture(&local, ".rustfs.sys/tmp", "part-remote").await;
            let (sv, sp) = fixture.source();
            let (dv, dp) = fixture.destination();
            ecstore_disk::DiskAPI::prepare_part_transaction(remote.as_ref(), sv, sp, dv, dp, fixture.metadata[1].clone())
                .await
                .expect("remote prepare");
            assert!(fixture.transaction().is_dir());
            assert_eq!(tokio::fs::read(&fixture.dst).await.expect("old data before publish"), b"aaaa");
            for (name, bytes) in &fixture.indices {
                assert_eq!(
                    tokio::fs::read(fixture.dst.parent().expect("parent").join(name))
                        .await
                        .expect("both generations retained during prepare"),
                    *bytes
                );
            }
            ecstore_disk::DiskAPI::rename_part(remote.as_ref(), sv, sp, dv, dp, fixture.metadata[1].clone())
                .await
                .expect("remote publish");
            assert!(!fixture.src.exists());
            assert_eq!(tokio::fs::read(&fixture.dst).await.expect("published data"), b"bbbb");
            assert_eq!(
                tokio::fs::read(fixture.dst.with_extension("1.meta"))
                    .await
                    .expect("published metadata"),
                fixture.metadata[1]
            );
            ecstore_disk::DiskAPI::settle_part_transaction(remote.as_ref(), dv, dp, action)
                .await
                .expect("remote settlement");
            assert_legacy_part_settled(&fixture, action).await;
        }
        let legacy_meta = Bytes::from(
            rustfs_filemeta::ObjectPartInfo {
                number: 1,
                size: 8,
                ..Default::default()
            }
            .marshal_msg()
            .expect("legacy metadata"),
        );
        let missing_path = "missing/part.1";
        assert_eq!(
            ecstore_disk::DiskAPI::prepare_part_transaction(
                remote.as_ref(),
                ".rustfs.sys/tmp",
                missing_path,
                "part-remote",
                missing_path,
                legacy_meta.clone()
            )
            .await
            .expect_err("missing source prepare response"),
            DiskError::FileNotFound
        );
        assert_eq!(
            ecstore_disk::DiskAPI::rename_part(
                remote.as_ref(),
                ".rustfs.sys/tmp",
                missing_path,
                "part-remote",
                missing_path,
                legacy_meta
            )
            .await
            .expect_err("missing source publish response"),
            DiskError::FileNotFound
        );
        let fixture = legacy_part_fixture(&local, ".rustfs.sys/tmp", "part-remote").await;
        tokio::fs::write(fixture.transaction(), b"not a transaction directory")
            .await
            .expect("corrupt transaction fixture");
        let before = legacy_part_snapshot(&fixture);
        assert_eq!(
            ecstore_disk::DiskAPI::settle_part_transaction(
                remote.as_ref(),
                &fixture.destination.0,
                &fixture.destination.1,
                PartTransactionAction::Rollback
            )
            .await
            .expect_err("corrupt settlement response"),
            DiskError::FileCorrupt
        );
        assert_eq!(legacy_part_snapshot(&fixture), before);
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            9,
            "six successful and three failed stages cross tonic"
        );
        ecstore_disk::DiskAPI::close(remote.as_ref())
            .await
            .expect("close remote client");
        drop(remote);
        shutdown.send(()).expect("stop server");
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("server drains")
            .expect("server task");
    }

    fn signed_legacy_rename_file_request(
        disk: &str,
        source: (&str, &str),
        destination: (&str, &str),
        durable: bool,
    ) -> Request<RenameFileRequest> {
        let mut request = Request::new(RenameFileRequest {
            disk: disk.to_owned(),
            src_volume: source.0.to_owned(),
            src_path: source.1.to_owned(),
            dst_volume: destination.0.to_owned(),
            dst_path: destination.1.to_owned(),
            durable,
        });
        let body = rustfs_protos::canonical_rename_file_request_body(request.get_ref()).expect("canonical rename-file body");
        sign_legacy_rpc_request(&mut request, &body, "RenameFile");
        request
    }

    async fn authenticated_legacy_rename_file(
        service: &NodeService,
        request: Request<RenameFileRequest>,
    ) -> Result<RenameFileResponse, Status> {
        verify_legacy_rpc_request(&request, "RenameFile")?;
        Ok(service.handle_rename_file(request).await?.into_inner())
    }

    async fn prepare_legacy_rename_file(
        disk: &ecstore_disk::DiskStore,
        source: (&str, &str),
        destination: (&str, &str),
    ) -> (std::path::PathBuf, std::path::PathBuf) {
        let root = ecstore_disk::DiskAPI::path(disk.as_ref());
        let source = root.join(source.0).join(source.1);
        let destination = root.join(destination.0).join(destination.1);
        for (path, bytes) in [
            (&source, LEGACY_RENAME_BODY),
            (&destination, b"previous destination".as_slice()),
        ] {
            tokio::fs::create_dir_all(path.parent().expect("fixture parent"))
                .await
                .expect("fixture directories");
            tokio::fs::write(path, bytes).await.expect("fixture bytes");
        }
        (source, destination)
    }

    async fn assert_legacy_rename_file_unchanged(source: &std::path::Path, destination: &std::path::Path) {
        assert_eq!(tokio::fs::read(source).await.expect("source retained"), LEGACY_RENAME_BODY);
        assert_eq!(tokio::fs::read(destination).await.expect("destination retained"), b"previous destination");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_rename_file_receiver_preserves_signed_flags_and_managed_rejection() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-rename-file-test-secret".to_owned());
        let root = tempfile::tempdir().expect("rename-file receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("rename-file-ordinary", true).await;
        env.ecstore
            .clone()
            .make_artifact_bucket("rename-file-managed", "outputs/")
            .await
            .expect("managed bucket");
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = service
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("receiving disk");
        for durable in [false, true] {
            for (src_volume, dst_volume) in [
                ("rename-file-ordinary", "rename-file-ordinary"),
                (".rustfs.sys/tmp", ".rustfs.sys"),
                (".rustfs.sys/tmp", "rename-file-ordinary"),
                ("rename-file-managed", "rename-file-ordinary"),
                ("rename-file-ordinary", "rename-file-managed"),
            ] {
                let prefix = format!("outputs/74656e616e74/{}/{}", Uuid::new_v4(), Uuid::new_v4());
                let src_path = format!("{prefix}/source");
                let dst_path = format!("{prefix}/destination");
                let source = (src_volume, src_path.as_str());
                let destination = (dst_volume, dst_path.as_str());
                let (src, dst) = prepare_legacy_rename_file(&disk, source, destination).await;
                let mut tampered = signed_legacy_rename_file_request(&disk_ref, source, destination, !durable);
                tampered.get_mut().durable = durable;
                assert_eq!(
                    authenticated_legacy_rename_file(&service, tampered)
                        .await
                        .expect_err("durability bit is signed")
                        .code(),
                    tonic::Code::PermissionDenied
                );
                assert_legacy_rename_file_unchanged(&src, &dst).await;
                let result = authenticated_legacy_rename_file(
                    &service,
                    signed_legacy_rename_file_request(&disk_ref, source, destination, durable),
                )
                .await;
                if src_volume == "rename-file-managed" || dst_volume == "rename-file-managed" {
                    assert_eq!(result.expect_err("both managed directions reject").code(), tonic::Code::PermissionDenied);
                    assert_legacy_rename_file_unchanged(&src, &dst).await;
                } else {
                    let response = result.expect("signed rename response");
                    assert!(response.success, "{:?}", response.error);
                    assert!(response.error.is_none());
                    assert_eq!(response.durability_applied, durable);
                    assert!(!src.exists());
                    assert_eq!(tokio::fs::read(dst).await.expect("published bytes"), LEGACY_RENAME_BODY);
                }
            }
            let response = authenticated_legacy_rename_file(
                &service,
                signed_legacy_rename_file_request(
                    "missing-disk",
                    ("rename-file-ordinary", "source"),
                    ("rename-file-ordinary", "destination"),
                    durable,
                ),
            )
            .await
            .expect("unknown disk envelope");
            assert!(!response.success && !response.durability_applied);
            assert!(response.error.expect("unknown disk").error_info.contains("cannot find disk"));
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_rename_file_receiver_rejects_missing_live_source_or_destination() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-rename-file-test-secret".to_owned());
        let root = tempfile::tempdir().expect("rename-file missing root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let service = service_for(&env).await;
        let target = service.local_mutation_target();
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("receiving disk");
        for durable in [false, true] {
            for missing_source in [false, true] {
                let src_volume = format!("rename-src-{durable}-{missing_source}");
                let dst_volume = format!("rename-dst-{durable}-{missing_source}");
                env.make_bucket(&src_volume, true).await;
                env.make_bucket(&dst_volume, true).await;
                let source = (src_volume.as_str(), "nested/source");
                let destination = (dst_volume.as_str(), "nested/destination");
                let (src, dst) = prepare_legacy_rename_file(&disk, source, destination).await;
                let missing = if missing_source { &src_volume } else { &dst_volume };
                let incarnation = env
                    .ecstore
                    .bucket_incarnation_id(missing)
                    .await
                    .expect("original incarnation");
                disk.delete_volume(missing, true).await.expect("remove only physical volume");
                assert_eq!(
                    env.ecstore
                        .bucket_incarnation_id(missing)
                        .await
                        .expect("retained bucket metadata"),
                    incarnation
                );
                target
                    .reject_legacy_artifact_mutation(missing)
                    .await
                    .expect("metadata still classifies ordinary");
                let response = authenticated_legacy_rename_file(
                    &service,
                    signed_legacy_rename_file_request(&disk_ref, source, destination, durable),
                )
                .await
                .expect("missing-volume envelope");
                assert!(!response.success && !response.durability_applied);
                assert_eq!(
                    DiskError::from(response.error.expect("live-volume admission error")),
                    DiskError::VolumeNotFound
                );
                assert!(!env.disk_paths[0].join(missing).exists(), "must not recreate missing physical volume");
                if missing_source {
                    assert_eq!(tokio::fs::read(dst).await.expect("destination remains"), b"previous destination");
                } else {
                    assert_eq!(tokio::fs::read(src).await.expect("source remains"), LEGACY_RENAME_BODY);
                }
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_rename_file_receiver_binds_instance_and_bootstrap() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-rename-file-test-secret".to_owned());
        let root = tempfile::tempdir().expect("rename-file instance root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("rename-file-instance", true).await;
        let ready = service_for(&env).await;
        let instance = crate::storage::storage_api::bootstrap_instance_ctx();
        assert!(super::super::BootstrapLocalTarget::new(instance.clone()).is_for_store(&env.ecstore));
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = ready
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("instance disk");
        let slot = super::super::ServerContextSlot::with_instance_context(instance.clone());
        let bootstrap = super::super::make_server_for_slot(slot.clone());
        let unbound = super::super::make_server_for_slot(super::super::ServerContextSlot::new());
        let mut wrong = super::super::make_server_for_context(ready.context.clone());
        wrong.server_ctx = Some(super::super::ServerContextSlot::with_instance_context(
            crate::storage::storage_api::new_instance_ctx(),
        ));
        for durable in [false, true] {
            for (src_volume, dst_volume) in [
                (".rustfs.sys/tmp", ".rustfs.sys"),
                (".rustfs.sys/tmp", "rename-file-instance"),
                ("rename-file-instance", ".rustfs.sys"),
            ] {
                let prefix = Uuid::new_v4().to_string();
                let src_path = format!("binding/{prefix}/source");
                let dst_path = format!("binding/{prefix}/destination");
                let source = (src_volume, src_path.as_str());
                let destination = (dst_volume, dst_path.as_str());
                let (src, dst) = prepare_legacy_rename_file(&disk, source, destination).await;
                for service in [&unbound, &wrong] {
                    let response = authenticated_legacy_rename_file(
                        service,
                        signed_legacy_rename_file_request(&disk_ref, source, destination, durable),
                    )
                    .await
                    .expect("unbound envelope");
                    assert!(!response.success && !response.durability_applied);
                    assert!(response.error.is_some());
                    assert_legacy_rename_file_unchanged(&src, &dst).await;
                }
                let response = authenticated_legacy_rename_file(
                    &bootstrap,
                    signed_legacy_rename_file_request(&disk_ref, source, destination, durable),
                )
                .await
                .expect("bootstrap envelope");
                if src_volume == ".rustfs.sys/tmp" && dst_volume == ".rustfs.sys" {
                    assert!(response.success, "{:?}", response.error);
                    assert!(response.error.is_none());
                    assert_eq!(response.durability_applied, durable);
                    assert!(!src.exists());
                    assert_eq!(tokio::fs::read(dst).await.expect("internal bytes"), LEGACY_RENAME_BODY);
                } else {
                    assert!(!response.success && !response.durability_applied);
                    assert_eq!(
                        DiskError::from(response.error.expect("bootstrap rejects either ordinary volume")),
                        DiskError::FileAccessDenied
                    );
                    assert_legacy_rename_file_unchanged(&src, &dst).await;
                }
            }
        }
        let captured = bootstrap.local_mutation_target();
        slot.try_install(ready.context.clone().expect("ready context"))
            .expect("install after capture");
        assert!(matches!(bootstrap.local_mutation_target(), LocalMutationTarget::Ready(_)));
        let source = ("rename-file-instance", "replacement/source");
        let destination = ("rename-file-instance", "replacement/destination");
        let (src, dst) = prepare_legacy_rename_file(&disk, source, destination).await;
        assert_eq!(
            captured
                .begin_legacy_local_rename_file(&disk_ref, source, destination, true)
                .await
                .err()
                .expect("captured bootstrap cannot upgrade"),
            DiskError::FileAccessDenied
        );
        let admitted = ready
            .local_mutation_target()
            .begin_legacy_local_rename_file(&disk_ref, source, destination, true)
            .await
            .expect("capture original disk");
        crate::storage::storage_api::init_local_disks_with_instance_ctx(&instance, env.endpoint_pools.clone())
            .await
            .expect("replace registry handles");
        let replacement = ready
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("replacement disk");
        assert!(!Arc::ptr_eq(&disk, &replacement));
        admitted.commit().await.expect("admitted rename retains original owner");
        assert!(!src.exists());
        assert_eq!(tokio::fs::read(&dst).await.expect("admitted bytes"), LEGACY_RENAME_BODY);
        let response =
            authenticated_legacy_rename_file(&ready, signed_legacy_rename_file_request(&disk_ref, destination, source, false))
                .await
                .expect("next request uses replacement");
        assert!(response.success && response.error.is_none());
        assert!(!response.durability_applied);
        assert!(!dst.exists());
        assert_eq!(tokio::fs::read(src).await.expect("returned bytes"), LEGACY_RENAME_BODY);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_rename_file_flow_remote_ordinary_and_durable() {
        use crate::storage::storage_api::{
            Endpoint, EndpointServerPools, Endpoints,
            ecstore_disk::{DiskOption, new_disk},
            init_local_disks_with_instance_ctx,
        };
        use rustfs_protos::proto_gen::node_service::node_service_server::NodeServiceServer;
        use tokio_stream::wrappers::TcpListenerStream;
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-rename-file-remote-secret".to_owned());
        let root = tempfile::tempdir().expect("remote rename-file root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("rename-file-remote", true).await;
        let ready = service_for(&env).await;
        let instance = crate::storage::storage_api::bootstrap_instance_ctx();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("loopback listener");
        let addr = listener.local_addr().expect("loopback address");
        let mut endpoints = env.endpoint_pools.as_ref()[0].endpoints.as_ref().clone();
        let mut endpoint =
            Endpoint::try_from(format!("http://{addr}{}", env.disk_paths[0].display()).as_str()).expect("remote endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        endpoint.is_local = true;
        endpoints[0] = endpoint.clone();
        let mut pool = env.endpoint_pools.as_ref()[0].clone();
        pool.endpoints = Endpoints::from(endpoints);
        init_local_disks_with_instance_ctx(&instance, EndpointServerPools::from(vec![pool]))
            .await
            .expect("register receiving disk");
        let service = super::super::make_server_for_context(ready.context.clone());
        let local = service
            .local_mutation_target()
            .find_mutation_disk(&endpoint.to_string())
            .await
            .expect("local receiving disk");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        let audience = addr.to_string();
        let auth = tower::ServiceBuilder::new().map_request(move |request: http::Request<_>| {
            let path = request.uri().path();
            crate::storage::storage_api::ecstore_rpc::verify_tonic_rpc_signature_with_bootstrap(
                &audience,
                path,
                request.headers(),
                path == "/node_service.NodeService/Ping",
            )
            .expect("real remote HMAC signature");
            if path == "/node_service.NodeService/RenameFile" {
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            request
        });
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .layer(auth)
                .add_service(NodeServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                })
                .await
                .expect("rename-file server");
        });
        endpoint.is_local = false;
        let remote = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("real remote client");
        assert!(!ecstore_disk::DiskAPI::is_local(remote.as_ref()));
        for durable in [false, true] {
            let src_path = format!("rename-file/{durable}/source");
            let dst_path = format!("rename-file/{durable}/destination");
            let source = (".rustfs.sys/tmp", src_path.as_str());
            let destination = ("rename-file-remote", dst_path.as_str());
            let (src, dst) = prepare_legacy_rename_file(&local, source, destination).await;
            if durable {
                ecstore_disk::DiskAPI::rename_file_durable(remote.as_ref(), source.0, source.1, destination.0, destination.1)
                    .await
                    .expect("durable response acknowledges publication");
            } else {
                ecstore_disk::DiskAPI::rename_file(remote.as_ref(), source.0, source.1, destination.0, destination.1)
                    .await
                    .expect("ordinary remote publication");
            }
            assert!(!src.exists());
            assert_eq!(tokio::fs::read(&dst).await.expect("remote published bytes"), LEGACY_RENAME_BODY);
        }
        let failed_destination = env.disk_paths[0].join("rename-file-remote/failed/destination");
        let error = ecstore_disk::DiskAPI::rename_file_durable(
            remote.as_ref(),
            ".rustfs.sys/tmp",
            "rename-file/missing",
            "rename-file-remote",
            "failed/destination",
        )
        .await
        .expect_err("missing source durable request fails");
        assert_eq!(error, DiskError::FileNotFound);
        assert!(!failed_destination.exists());
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "all rename-file operations crossed tonic"
        );
        ecstore_disk::DiskAPI::close(remote.as_ref())
            .await
            .expect("close remote client");
        drop(remote);
        shutdown.send(()).expect("stop server");
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("server drains")
            .expect("server task");
    }

    fn signed_legacy_bulk_request(
        disk: &str,
        volume: &str,
        versions: &[FileInfoVersions],
        opts: &DeleteOptions,
        msgpack: bool,
    ) -> Request<DeleteVersionsRequest> {
        let mut request = Request::new(DeleteVersionsRequest {
            disk: disk.to_owned(),
            volume: volume.to_owned(),
            versions: versions
                .iter()
                .map(|group| {
                    if msgpack {
                        "binary must take precedence".to_owned()
                    } else {
                        serde_json::to_string(group).expect("JSON version group")
                    }
                })
                .collect(),
            versions_bin: if msgpack {
                versions
                    .iter()
                    .map(|group| {
                        encode_msgpack_named(group, "FileInfoVersions")
                            .expect("binary version group")
                            .into()
                    })
                    .collect()
            } else {
                Vec::new()
            },
            opts: if msgpack {
                "binary must take precedence".to_owned()
            } else {
                serde_json::to_string(opts).expect("JSON bulk options")
            },
            opts_bin: if msgpack {
                encode_msgpack_named(opts, "DeleteOptions")
                    .expect("binary bulk options")
                    .into()
            } else {
                Bytes::new()
            },
        });
        let body = rustfs_protos::canonical_delete_versions_request_body(request.get_ref()).expect("canonical bulk body");
        sign_legacy_rpc_request(&mut request, &body, "DeleteVersions");
        request
    }

    async fn authenticated_legacy_bulk(
        service: &NodeService,
        request: Request<DeleteVersionsRequest>,
    ) -> Result<DeleteVersionsResponse, Status> {
        verify_legacy_rpc_request(&request, "DeleteVersions")?;
        Ok(service.handle_delete_versions(request).await?.into_inner())
    }

    fn assert_bulk_results(response: &DeleteVersionsResponse, expected: &[Option<DiskError>]) {
        assert!(response.success, "bulk envelope: {:?}", response.error);
        assert!(response.error.is_none());
        assert_eq!(response.errors.len(), expected.len());
        assert_eq!(response.item_errors.len(), expected.len());
        for ((text, typed), expected) in response.errors.iter().zip(&response.item_errors).zip(expected) {
            match expected {
                Some(error) => {
                    assert_eq!(typed.code, error.to_u32());
                    assert_eq!(&typed.error_info, text);
                    assert!(!text.is_empty());
                }
                None => {
                    assert!(text.is_empty());
                    assert_eq!(typed.code, 0);
                    assert!(typed.error_info.is_empty());
                }
            }
        }
    }

    async fn prepare_legacy_bulk(
        disk: &ecstore_disk::DiskStore,
        volume: &str,
        prefix: &str,
    ) -> (Vec<FileInfoVersions>, Vec<(std::path::PathBuf, Vec<u8>)>) {
        let one = format!("{prefix}/history");
        let (latest, previous) = stage_legacy_delete(disk, volume, &one, "remaining").await;
        let previous = previous.expect("two original versions").0;
        let two = format!("{prefix}/data");
        let (data, _) = prepare_legacy_delete_rollback(disk, volume, &two, "last").await;
        let groups = vec![
            FileInfoVersions {
                volume: volume.to_owned(),
                name: one,
                versions: vec![latest, previous],
                ..Default::default()
            },
            FileInfoVersions {
                volume: volume.to_owned(),
                name: two,
                versions: vec![data],
                ..Default::default()
            },
        ];
        let mut before = Vec::new();
        for group in &groups {
            let object = ecstore_disk::DiskAPI::path(disk.as_ref()).join(volume).join(&group.name);
            let metadata = object.join("xl.meta");
            before.push((metadata.clone(), tokio::fs::read(&metadata).await.expect("complete original metadata")));
            for fi in &group.versions {
                if let Some(data_dir) = fi.data_dir {
                    let part = object.join(data_dir.to_string()).join("part.1");
                    before.push((part.clone(), tokio::fs::read(&part).await.expect("original shard")));
                }
            }
        }
        (groups, before)
    }

    async fn assert_bulk_bytes(before: &[(std::path::PathBuf, Vec<u8>)]) {
        for (path, bytes) in before {
            assert_eq!(tokio::fs::read(path).await.expect("original file exists"), *bytes, "{}", path.display());
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn delete_recovery_and_guarded_receivers_reject_managed_mutations() {
        use crate::storage::storage_api::ecstore_disk::{CurrentDeleteGeneration, DeleteRecoveryAction, GuardedFileInfoVersions};
        let root = tempfile::tempdir().expect("managed recovery receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let bucket = "recovery-managed";
        let object = "outputs/74656e616e74/11111111-1111-4111-8111-111111111111/22222222-2222-4222-8222-222222222222/object";
        env.ecstore
            .clone()
            .make_artifact_bucket(bucket, "outputs/")
            .await
            .expect("real managed authority");
        env.put_object_bytes(bucket, object, vec![0x42; 4097]).await;
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = service
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("captured target");
        let fi = ecstore_disk::DiskAPI::read_version(
            disk.as_ref(),
            bucket,
            bucket,
            object,
            "",
            &ReadOptions {
                read_data: true,
                ..Default::default()
            },
        )
        .await
        .expect("real published version");
        let group = FileInfoVersions {
            volume: bucket.to_owned(),
            name: object.to_owned(),
            versions: vec![fi.clone()],
            ..Default::default()
        };
        let opts = DeleteOptions {
            old_data_dir: Some(Uuid::new_v4()),
            ..Default::default()
        };
        let before = path_delete_snapshot(&env.disk_paths[0].join(bucket));
        for operation in [
            DeleteRecoveryRpcOperation::ApplyVersion {
                object: object.to_owned(),
                file_info: Box::new(fi.clone()),
                force_del_marker: false,
                opts: opts.clone(),
            },
            DeleteRecoveryRpcOperation::ApplyVersions {
                versions: vec![group.clone()],
                opts: opts.clone(),
            },
            DeleteRecoveryRpcOperation::Settle {
                object: object.to_owned(),
                transaction: opts.old_data_dir.expect("transaction"),
                expected_fingerprint: [0; 32],
                action: DeleteRecoveryAction::Rollback,
            },
        ] {
            let mut request = Request::new(RecoverDeleteTransactionRequest {
                disk: disk_ref.clone(),
                volume: bucket.to_owned(),
                protocol_version: DELETE_RECOVERY_PROTOCOL_VERSION,
                operation: rmp_serde::to_vec_named(&operation).expect("recovery operation").into(),
            });
            let canonical =
                rustfs_protos::canonical_recover_delete_transaction_request_body(request.get_ref()).expect("recovery body");
            set_tonic_canonical_body_digest(&mut request, &canonical).expect("recovery digest");
            mark_v2_authenticated(&mut request);
            assert_eq!(
                service
                    .handle_recover_delete_transaction(request)
                    .await
                    .expect_err("managed recovery mutation")
                    .code(),
                tonic::Code::PermissionDenied
            );
            assert_eq!(path_delete_snapshot(&env.disk_paths[0].join(bucket)), before);
        }
        let payload = GuardedDeleteVersionsPayload {
            versions: vec![GuardedFileInfoVersions {
                expected_current: CurrentDeleteGeneration::from_file_info(&fi).expect("actual generation"),
                group,
            }],
            opts,
        };
        payload.validate().expect("valid guarded mutation fixture");
        let mut request = Request::new(GuardedDeleteVersionsRequest {
            disk: disk_ref,
            volume: bucket.to_owned(),
            probe_only: false,
            payload: rmp_serde::to_vec_named(&payload).expect("guarded operation").into(),
        });
        let canonical = rustfs_protos::canonical_guarded_delete_versions_request_body(request.get_ref()).expect("guarded body");
        set_tonic_canonical_body_digest(&mut request, &canonical).expect("guarded digest");
        mark_v2_authenticated(&mut request);
        assert_eq!(
            service
                .handle_guarded_delete_versions(request)
                .await
                .expect_err("managed guarded mutation")
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(path_delete_snapshot(&env.disk_paths[0].join(bucket)), before);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn artifact_delete_recovery_receivers_apply_and_rollback_real_ordinary_versions() {
        use crate::storage::storage_api::ecstore_disk::{CurrentDeleteGeneration, DeleteRecoveryAction, GuardedFileInfoVersions};
        use crate::storage::storage_api::{ObjectOptions, PutObjReader, contract::object::ObjectIO as _};
        use std::time::Duration;

        async fn recover(
            service: &NodeService,
            disk: &str,
            bucket: &str,
            operation: DeleteRecoveryRpcOperation,
        ) -> DeleteRecoveryRpcReply {
            let mut request = Request::new(RecoverDeleteTransactionRequest {
                disk: disk.to_owned(),
                volume: bucket.to_owned(),
                protocol_version: DELETE_RECOVERY_PROTOCOL_VERSION,
                operation: rmp_serde::to_vec_named(&operation).expect("named recovery operation").into(),
            });
            let body = rustfs_protos::canonical_recover_delete_transaction_request_body(request.get_ref())
                .expect("canonical recovery body");
            sign_legacy_rpc_request(&mut request, &body, "RecoverDeleteTransaction");
            verify_legacy_rpc_request(&request, "RecoverDeleteTransaction").expect("real recovery HMAC and replay scope");
            let response = tokio::time::timeout(Duration::from_secs(30), service.handle_recover_delete_transaction(request))
                .await
                .expect("bounded recovery operation")
                .expect("authenticated recovery response")
                .into_inner();
            assert!(response.success, "recovery operation failed: {:?}", response.error);
            assert!(response.error.is_none());
            assert_eq!(response.protocol_version, DELETE_RECOVERY_PROTOCOL_VERSION);
            rustfs_protos::decode_delete_recovery_payload(&response.result).expect("typed recovery reply")
        }

        let _ = rustfs_credentials::set_global_rpc_secret("ordinary-delete-recovery-test-secret".to_owned());
        let root = tempfile::tempdir().expect("ordinary recovery receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let bucket = "delete-recovery-ordinary";
        env.make_bucket(bucket, true).await;
        let incarnation = env
            .ecstore
            .bucket_incarnation_id(bucket)
            .await
            .expect("real ordinary incarnation");
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = service
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("local receiver disk");
        for mode in ["single", "batch", "guarded"] {
            let object = format!("recovery/{mode}");
            let payload = vec![0x42; 257];
            env.ecstore
                .put_object(
                    bucket,
                    &object,
                    &mut PutObjReader::from_vec(payload),
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("real versioned ordinary PUT");
            let fi = disk
                .read_version(
                    bucket,
                    bucket,
                    &object,
                    "",
                    &ReadOptions {
                        read_data: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("real PUT generation on the receiving disk");
            assert!(fi.inline_data(), "small real PUT must produce a complete inline rollback fixture");
            assert!(
                fi.data.as_ref().is_some_and(|data| !data.is_empty()),
                "real encoded inline shard is present"
            );
            let physical = env.disk_paths[0].join(bucket).join(&object);
            let before = path_delete_snapshot(&physical);
            let transaction = Uuid::new_v4();
            let opts = DeleteOptions {
                old_data_dir: Some(transaction),
                delete_recovery_bucket_incarnation: Some(incarnation),
                ..Default::default()
            };
            let group = FileInfoVersions {
                volume: bucket.to_owned(),
                name: object.clone(),
                versions: vec![fi.clone()],
                ..Default::default()
            };
            if mode == "guarded" {
                let payload = GuardedDeleteVersionsPayload {
                    versions: vec![GuardedFileInfoVersions {
                        expected_current: CurrentDeleteGeneration::from_file_info(&fi).expect("actual current generation"),
                        group,
                    }],
                    opts,
                };
                payload.validate().expect("valid guarded ordinary request");
                let mut request = Request::new(GuardedDeleteVersionsRequest {
                    disk: disk_ref.clone(),
                    volume: bucket.to_owned(),
                    probe_only: false,
                    payload: rmp_serde::to_vec_named(&payload).expect("named guarded request").into(),
                });
                let body = rustfs_protos::canonical_guarded_delete_versions_request_body(request.get_ref())
                    .expect("canonical guarded request");
                sign_legacy_rpc_request(&mut request, &body, "GuardedDeleteVersions");
                verify_legacy_rpc_request(&request, "GuardedDeleteVersions").expect("real guarded HMAC and replay scope");
                let response = tokio::time::timeout(Duration::from_secs(30), service.handle_guarded_delete_versions(request))
                    .await
                    .expect("bounded guarded publication")
                    .expect("authenticated guarded response")
                    .into_inner();
                assert_bulk_results(&response, &[None]);
            } else {
                let operation = if mode == "single" {
                    DeleteRecoveryRpcOperation::ApplyVersion {
                        object: object.clone(),
                        file_info: Box::new(fi.clone()),
                        force_del_marker: false,
                        opts,
                    }
                } else {
                    DeleteRecoveryRpcOperation::ApplyVersions {
                        versions: vec![group],
                        opts,
                    }
                };
                match recover(&service, &disk_ref, bucket, operation).await {
                    DeleteRecoveryRpcReply::AppliedVersion if mode == "single" => {}
                    DeleteRecoveryRpcReply::AppliedVersions(errors) if mode == "batch" => assert_eq!(errors, vec![None]),
                    reply => panic!("unexpected apply reply: {reply:?}"),
                }
            }
            let DeleteRecoveryRpcReply::Inspected(state) = recover(
                &service,
                &disk_ref,
                bucket,
                DeleteRecoveryRpcOperation::Inspect {
                    object: object.clone(),
                    transaction,
                },
            )
            .await
            else {
                panic!("typed inspection reply");
            };
            assert_eq!(serde_json::to_value(&state.state).expect("serialized recovery state"), "NewPrepared");
            assert_eq!(state.current_digest, state.journal.as_ref().expect("durable journal").target_digest);
            assert!(!physical.join("xl.meta").exists(), "real selected version was removed");
            assert_ne!(path_delete_snapshot(&physical), before, "forward operation must change physical state");
            let DeleteRecoveryRpcReply::Settled(restored) = recover(
                &service,
                &disk_ref,
                bucket,
                DeleteRecoveryRpcOperation::Settle {
                    object: object.clone(),
                    transaction,
                    expected_fingerprint: state.fingerprint,
                    action: DeleteRecoveryAction::Rollback,
                },
            )
            .await
            else {
                panic!("typed rollback reply");
            };
            assert_eq!(serde_json::to_value(&restored.state).expect("serialized recovery state"), "Absent");
            assert_eq!(
                path_delete_snapshot(&physical),
                before,
                "rollback restores exact metadata and removes journal residue"
            );
            let restored = disk
                .read_version(
                    bucket,
                    bucket,
                    &object,
                    &fi.version_id.expect("real version").to_string(),
                    &ReadOptions {
                        read_data: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("exact restored ordinary version");
            assert_eq!(restored.version_id, fi.version_id);
            assert_eq!(restored.data, fi.data);
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_bulk_delete_receiver_preserves_signed_group_results_and_rejects_managed() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-bulk-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("bulk receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("bulk-ordinary", true).await;
        env.ecstore
            .clone()
            .make_artifact_bucket("bulk-managed", "outputs/")
            .await
            .expect("managed bucket");
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = service
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("local disk");
        for msgpack in [false, true] {
            for volume in ["bulk-ordinary", ".rustfs.sys", "bulk-managed"] {
                let prefix = if volume == "bulk-managed" {
                    format!("outputs/74656e616e74/{}/{}", Uuid::new_v4(), Uuid::new_v4())
                } else {
                    format!("bulk/{msgpack}")
                };
                let (groups, before) = prepare_legacy_bulk(&disk, volume, &prefix).await;
                let rollback = Uuid::new_v4();
                let opts = DeleteOptions {
                    old_data_dir: Some(rollback),
                    ..Default::default()
                };
                let mut input = groups.clone();
                input.insert(
                    1,
                    FileInfoVersions {
                        volume: volume.to_owned(),
                        name: format!("{prefix}/missing"),
                        ..Default::default()
                    },
                );
                if volume == "bulk-ordinary" {
                    let rejected = authenticated_legacy_bulk(
                        &service,
                        signed_legacy_bulk_request(&disk_ref, volume, &input, &opts, msgpack),
                    )
                    .await
                    .expect("obsolete ordinary transaction envelope");
                    assert!(!rejected.success);
                    assert!(rejected.errors.is_empty() && rejected.item_errors.is_empty());
                    assert_eq!(DiskError::from(rejected.error.expect("obsolete transaction")), DiskError::UnsupportedDisk);
                    assert_bulk_bytes(&before).await;
                    for group in &groups {
                        assert!(
                            !env.disk_paths[0]
                                .join(volume)
                                .join(&group.name)
                                .join(rollback.to_string())
                                .exists()
                        );
                    }
                }
                let current_opts = if volume == ".rustfs.sys" {
                    opts.clone()
                } else {
                    DeleteOptions::default()
                };
                let result = authenticated_legacy_bulk(
                    &service,
                    signed_legacy_bulk_request(&disk_ref, volume, &input, &current_opts, msgpack),
                )
                .await;
                if volume == "bulk-managed" {
                    assert_eq!(
                        result.expect_err("managed batch rejects before writes").code(),
                        tonic::Code::PermissionDenied
                    );
                    assert_bulk_bytes(&before).await;
                    for group in &groups {
                        assert!(
                            !env.disk_paths[0]
                                .join(volume)
                                .join(&group.name)
                                .join(rollback.to_string())
                                .exists()
                        );
                    }
                    continue;
                }
                assert_bulk_results(&result.expect("signed forward"), &[None, Some(DiskError::FileNotFound), None]);
                if volume == "bulk-ordinary" {
                    for (path, _) in &before {
                        assert!(!path.exists(), "ordinary nontransactional batch removes actual metadata and parts");
                    }
                    for group in &groups {
                        assert!(
                            !env.disk_paths[0]
                                .join(volume)
                                .join(&group.name)
                                .join(rollback.to_string())
                                .exists()
                        );
                    }
                    continue;
                }
                for (path, original) in &before {
                    if path.file_name().is_some_and(|name| name == "xl.meta") {
                        assert!(!path.exists(), "all selected versions removed");
                        let backup = path.parent().expect("object").join(rollback.to_string()).join("xl.meta.bkp");
                        assert_eq!(tokio::fs::read(backup).await.expect("one complete group backup"), *original);
                    }
                }
                let undo = DeleteOptions {
                    undo_write: true,
                    undo_delete: true,
                    ..opts
                };
                let response =
                    authenticated_legacy_bulk(&service, signed_legacy_bulk_request(&disk_ref, volume, &groups, &undo, msgpack))
                        .await
                        .expect("signed group undo");
                assert_bulk_results(&response, &[None, None]);
                assert_bulk_bytes(&before).await;
                for group in &groups {
                    assert!(
                        !env.disk_paths[0]
                            .join(volume)
                            .join(&group.name)
                            .join(rollback.to_string())
                            .exists()
                    );
                }
            }
        }
        let response = authenticated_legacy_bulk(
            &service,
            signed_legacy_bulk_request(&disk_ref, "bulk-ordinary", &[], &DeleteOptions::default(), false),
        )
        .await
        .expect("empty batch");
        assert_bulk_results(&response, &[]);
        assert_eq!(
            authenticated_legacy_bulk(
                &service,
                signed_legacy_bulk_request(&disk_ref, "bulk-managed", &[], &DeleteOptions::default(), false)
            )
            .await
            .expect_err("empty managed batch still classifies")
            .code(),
            tonic::Code::PermissionDenied
        );
        let mut rejected_empty = signed_legacy_bulk_request(&disk_ref, "bulk-managed", &[], &DeleteOptions::default(), false);
        rejected_empty.get_mut().opts = "invalid JSON".to_owned();
        let body = rustfs_protos::canonical_delete_versions_request_body(rejected_empty.get_ref()).expect("empty request body");
        sign_legacy_rpc_request(&mut rejected_empty, &body, "DeleteVersions");
        assert_eq!(
            authenticated_legacy_bulk(&service, rejected_empty)
                .await
                .expect_err("empty batch classifies before decoding as before")
                .code(),
            tonic::Code::PermissionDenied
        );
        for invalid_group in [true, false] {
            let mut request = signed_legacy_bulk_request(&disk_ref, "bulk-ordinary", &[], &DeleteOptions::default(), false);
            if invalid_group {
                request.get_mut().versions.push("invalid JSON".to_owned());
            } else {
                request.get_mut().opts = "invalid JSON".to_owned();
            }
            let body = rustfs_protos::canonical_delete_versions_request_body(request.get_ref())
                .expect("malformed payload has canonical wire bytes");
            sign_legacy_rpc_request(&mut request, &body, "DeleteVersions");
            let response = authenticated_legacy_bulk(&service, request)
                .await
                .expect("decode failure envelope");
            assert!(!response.success);
            assert!(response.errors.is_empty() && response.item_errors.is_empty());
            assert!(response.error.expect("decode error").error_info.contains(if invalid_group {
                "decode FileInfoVersions failed"
            } else {
                "decode DeleteOptions failed"
            }));
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_bulk_delete_receiver_rejects_missing_volume_with_retained_metadata() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-bulk-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("bulk missing-volume root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let service = service_for(&env).await;
        let target = service.local_mutation_target();
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("local disk");
        for msgpack in [false, true] {
            let volume = format!("bulk-missing-{msgpack}");
            env.make_bucket(&volume, true).await;
            let (groups, _) = prepare_legacy_bulk(&disk, &volume, "objects").await;
            let incarnation = env.ecstore.bucket_incarnation_id(&volume).await.expect("incarnation");
            disk.delete_volume(&volume, true).await.expect("remove physical volume");
            assert_eq!(env.ecstore.bucket_incarnation_id(&volume).await.expect("retained metadata"), incarnation);
            target.reject_legacy_artifact_mutation(&volume).await.expect("still ordinary");
            for undo in [false, true] {
                let opts = DeleteOptions {
                    old_data_dir: undo.then(Uuid::new_v4),
                    undo_write: undo,
                    undo_delete: undo,
                    ..Default::default()
                };
                let response =
                    authenticated_legacy_bulk(&service, signed_legacy_bulk_request(&disk_ref, &volume, &groups, &opts, msgpack))
                        .await
                        .expect("admission failure envelope");
                assert!(!response.success);
                assert!(response.errors.is_empty() && response.item_errors.is_empty());
                assert_eq!(DiskError::from(response.error.expect("missing volume")), DiskError::VolumeNotFound);
                assert!(!env.disk_paths[0].join(&volume).exists());
            }
            let response = authenticated_legacy_bulk(
                &service,
                signed_legacy_bulk_request(&disk_ref, &volume, &[], &DeleteOptions::default(), msgpack),
            )
            .await
            .expect("empty batch needs no live volume");
            assert_bulk_results(&response, &[]);
            assert!(!env.disk_paths[0].join(&volume).exists());
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_bulk_delete_receiver_binds_instance_and_bootstrap_admission() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-bulk-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("bulk instance root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("bulk-instance", true).await;
        let ready = service_for(&env).await;
        let instance = crate::storage::storage_api::bootstrap_instance_ctx();
        assert!(super::super::BootstrapLocalTarget::new(instance.clone()).is_for_store(&env.ecstore));
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = ready
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("instance disk");
        let slot = super::super::ServerContextSlot::with_instance_context(instance.clone());
        let bootstrap = super::super::make_server_for_slot(slot.clone());
        let unbound = super::super::make_server_for_slot(super::super::ServerContextSlot::new());
        let mut wrong = super::super::make_server_for_context(ready.context.clone());
        wrong.server_ctx = Some(super::super::ServerContextSlot::with_instance_context(
            crate::storage::storage_api::new_instance_ctx(),
        ));
        for msgpack in [false, true] {
            for volume in ["bulk-instance", ".rustfs.sys"] {
                let (groups, before) = prepare_legacy_bulk(&disk, volume, &format!("binding/{msgpack}")).await;
                let opts = DeleteOptions {
                    old_data_dir: (volume == ".rustfs.sys").then(Uuid::new_v4),
                    ..Default::default()
                };
                for service in [&unbound, &wrong] {
                    let response = authenticated_legacy_bulk(
                        service,
                        signed_legacy_bulk_request(&disk_ref, volume, &groups, &opts, msgpack),
                    )
                    .await
                    .expect("unbound envelope");
                    assert!(!response.success);
                    assert!(response.error.is_some());
                    assert!(response.errors.is_empty() && response.item_errors.is_empty());
                    assert_bulk_bytes(&before).await;
                }
                let response =
                    authenticated_legacy_bulk(&bootstrap, signed_legacy_bulk_request(&disk_ref, volume, &groups, &opts, msgpack))
                        .await
                        .expect("bootstrap envelope");
                if volume == ".rustfs.sys" {
                    assert_bulk_results(&response, &[None, None]);
                    let undo = DeleteOptions {
                        undo_write: true,
                        undo_delete: true,
                        ..opts
                    };
                    assert_bulk_results(
                        &authenticated_legacy_bulk(
                            &bootstrap,
                            signed_legacy_bulk_request(&disk_ref, volume, &groups, &undo, msgpack),
                        )
                        .await
                        .expect("internal undo"),
                        &[None, None],
                    );
                } else {
                    assert!(!response.success);
                    assert_eq!(
                        DiskError::from(response.error.expect("bootstrap cannot write ordinary volume")),
                        DiskError::FileAccessDenied
                    );
                }
                assert_bulk_bytes(&before).await;
            }
        }
        let captured = bootstrap.local_mutation_target();
        slot.try_install(ready.context.clone().expect("ready context"))
            .expect("publish ready slot");
        assert!(matches!(bootstrap.local_mutation_target(), LocalMutationTarget::Ready(_)));
        assert_eq!(
            captured
                .begin_legacy_local_delete_versions(&disk_ref, "bulk-instance")
                .await
                .expect_err("captured bootstrap cannot upgrade")
                .to_u32(),
            DiskError::FileAccessDenied.to_u32()
        );
        let admitted = ready
            .local_mutation_target()
            .begin_legacy_local_delete_versions(&disk_ref, "bulk-instance")
            .await
            .expect("capture original disk before replacement");
        crate::storage::storage_api::init_local_disks_with_instance_ctx(&instance, env.endpoint_pools.clone())
            .await
            .expect("replace registry handles on the same disk paths");
        let replacement = ready
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("replacement disk");
        assert!(!Arc::ptr_eq(&disk, &replacement));
        let (groups, before) = prepare_legacy_bulk(&replacement, "bulk-instance", "replacement").await;
        let rollback = Uuid::new_v4();
        assert_eq!(
            admitted
                .delete_versions(
                    groups.clone(),
                    DeleteOptions {
                        old_data_dir: Some(rollback),
                        ..Default::default()
                    }
                )
                .await,
            vec![None, None]
        );
        let undo = DeleteOptions {
            old_data_dir: Some(rollback),
            undo_write: true,
            undo_delete: true,
            ..Default::default()
        };
        assert_bulk_results(
            &authenticated_legacy_bulk(&ready, signed_legacy_bulk_request(&disk_ref, "bulk-instance", &groups, &undo, true))
                .await
                .expect("replacement handles the next request"),
            &[None, None],
        );
        assert_bulk_bytes(&before).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_bulk_delete_flow_remote_forward_and_undo() {
        use crate::storage::storage_api::{
            Endpoint, EndpointServerPools, Endpoints,
            ecstore_disk::{DiskOption, new_disk},
            init_local_disks_with_instance_ctx,
        };
        use rustfs_protos::proto_gen::node_service::node_service_server::NodeServiceServer;
        use tokio_stream::wrappers::TcpListenerStream;
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-bulk-remote-test-secret".to_owned());
        let root = tempfile::tempdir().expect("bulk remote root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let ready = service_for(&env).await;
        let instance = crate::storage::storage_api::bootstrap_instance_ctx();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("loopback listener");
        let addr = listener.local_addr().expect("loopback address");
        let mut endpoints = env.endpoint_pools.as_ref()[0].endpoints.as_ref().clone();
        let mut endpoint =
            Endpoint::try_from(format!("http://{addr}{}", env.disk_paths[0].display()).as_str()).expect("remote endpoint");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        endpoint.is_local = true;
        endpoints[0] = endpoint.clone();
        let mut pool = env.endpoint_pools.as_ref()[0].clone();
        pool.endpoints = Endpoints::from(endpoints);
        init_local_disks_with_instance_ctx(&instance, EndpointServerPools::from(vec![pool]))
            .await
            .expect("register loopback disk");
        let service = super::super::make_server_for_context(ready.context.clone());
        let local = service
            .local_mutation_target()
            .find_mutation_disk(&endpoint.to_string())
            .await
            .expect("loopback receiving disk");
        let (groups, before) = prepare_legacy_bulk(&local, ".rustfs.sys", "objects").await;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        let audience = addr.to_string();
        let auth = tower::ServiceBuilder::new().map_request(move |request: http::Request<_>| {
            let path = request.uri().path();
            crate::storage::storage_api::ecstore_rpc::verify_tonic_rpc_signature_with_bootstrap(
                &audience,
                path,
                request.headers(),
                path == "/node_service.NodeService/Ping",
            )
            .expect("real remote HMAC signature");
            if path == "/node_service.NodeService/DeleteVersions" {
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            request
        });
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .layer(auth)
                .add_service(NodeServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                })
                .await
                .expect("loopback server");
        });
        endpoint.is_local = false;
        let remote = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("real RemoteDisk client");
        assert!(!ecstore_disk::DiskAPI::is_local(remote.as_ref()));
        let rollback = Uuid::new_v4();
        let mut input = groups.clone();
        input.insert(
            1,
            FileInfoVersions {
                volume: ".rustfs.sys".to_owned(),
                name: "objects/missing".to_owned(),
                ..Default::default()
            },
        );
        let forward = remote
            .delete_versions(
                ".rustfs.sys",
                input,
                DeleteOptions {
                    old_data_dir: Some(rollback),
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(
            forward,
            vec![None, Some(DiskError::FileNotFound), None],
            "typed per-group errors cross the real wire"
        );
        for (path, bytes) in &before {
            if path.file_name().is_some_and(|name| name == "xl.meta") {
                assert!(!path.exists());
                assert_eq!(
                    tokio::fs::read(path.parent().expect("object").join(rollback.to_string()).join("xl.meta.bkp"))
                        .await
                        .expect("complete backup"),
                    *bytes
                );
            }
        }
        let undo = remote
            .delete_versions(
                ".rustfs.sys",
                groups,
                DeleteOptions {
                    old_data_dir: Some(rollback),
                    undo_write: true,
                    undo_delete: true,
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(undo, vec![None, None]);
        assert_bulk_bytes(&before).await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2, "forward and undo used tonic");
        ecstore_disk::DiskAPI::close(remote.as_ref())
            .await
            .expect("close remote handle");
        drop(remote);
        shutdown.send(()).expect("stop loopback server");
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("server drained")
            .expect("server task");
    }

    async fn stage_legacy_undo(
        disk: &ecstore_disk::DiskStore,
        volume: &str,
        path: &str,
        case: &str,
    ) -> (FileInfo, DeleteOptions, Option<(FileInfo, Vec<u8>)>) {
        let mut incoming = inline_rename_file_info(path);
        let previous = if case == "fresh" {
            None
        } else {
            let mut previous = incoming.clone();
            if case == "remaining" {
                previous.version_id = Some(Uuid::new_v4());
            }
            ecstore_disk::DiskAPI::write_metadata(disk.as_ref(), "", volume, path, previous.clone())
                .await
                .expect("original real inline version");
            let bytes = tokio::fs::read(
                ecstore_disk::DiskAPI::path(disk.as_ref())
                    .join(volume)
                    .join(path)
                    .join("xl.meta"),
            )
            .await
            .expect("original metadata bytes");
            Some((previous, bytes))
        };
        let body = b"complete new rollback payload";
        incoming.data = Some(Bytes::from_static(body));
        incoming.size = i64::try_from(body.len()).expect("incoming size");
        incoming.parts[0].size = body.len();
        incoming.parts[0].actual_size = incoming.size;
        incoming.validate_for_metadata_read().expect("valid new inline version");
        let source = format!("undo-stage/{}", Uuid::new_v4());
        ecstore_disk::DiskAPI::write_metadata(disk.as_ref(), "", ".rustfs.sys/tmp", &source, incoming.clone())
            .await
            .expect("real staged inline metadata");
        let committed =
            ecstore_disk::DiskAPI::rename_data(disk.as_ref(), ".rustfs.sys/tmp", &source, incoming.clone(), volume, path)
                .await
                .expect("real RenameData publication before undo");
        assert_eq!(committed.rollback_data_dir.is_some(), case == "backup");
        let actual = disk
            .read_version(
                volume,
                volume,
                path,
                &incoming.version_id.expect("new version").to_string(),
                &ReadOptions {
                    read_data: true,
                    ..Default::default()
                },
            )
            .await
            .expect("new version really published");
        assert_eq!(actual.data, incoming.data);
        assert_eq!(actual.version_id, incoming.version_id);
        let opts = DeleteOptions {
            undo_write: true,
            old_data_dir: committed.rollback_data_dir,
            ..Default::default()
        };
        (incoming, opts, previous)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_undo_write_receiver_preserves_rollback_versions_and_rejects_managed() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-undo-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("undo receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("undo-ordinary", true).await;
        env.ecstore
            .clone()
            .make_artifact_bucket("undo-managed", "outputs/")
            .await
            .expect("managed bucket");
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = service
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("listener local disk");
        for msgpack in [false, true] {
            for volume in ["undo-ordinary", ".rustfs.sys", "undo-managed"] {
                for case in ["backup", "remaining", "fresh"] {
                    let path = if volume == "undo-managed" {
                        format!("outputs/74656e616e74/{}/{}/{case}-{msgpack}", Uuid::new_v4(), Uuid::new_v4())
                    } else {
                        format!("undo/{case}-{msgpack}")
                    };
                    let (incoming, opts, previous) = stage_legacy_undo(&disk, volume, &path, case).await;
                    let physical = env.disk_paths[0].join(volume).join(&path).join("xl.meta");
                    let before = tokio::fs::read(&physical).await.expect("published metadata");
                    let backup = opts.old_data_dir.map(|id| {
                        physical
                            .parent()
                            .expect("object directory")
                            .join(id.to_string())
                            .join("xl.meta.bkp")
                    });
                    let result = authenticated_legacy_delete(
                        &service,
                        signed_legacy_delete_request(&disk_ref, volume, &path, &incoming, &opts, msgpack, false),
                    )
                    .await;
                    if volume == "undo-managed" {
                        assert_eq!(result.expect_err("managed undo must reject").code(), tonic::Code::PermissionDenied);
                        assert_eq!(tokio::fs::read(&physical).await.expect("managed metadata retained"), before);
                        if let Some(backup) = backup {
                            assert_eq!(
                                tokio::fs::read(backup).await.expect("managed rollback backup retained"),
                                previous.as_ref().expect("previous version").1
                            );
                        }
                        continue;
                    }
                    let response = result.expect("signed undo response").into_inner();
                    assert!(response.success, "volume={volume}, case={case}, msgpack={msgpack}: {:?}", response.error);
                    assert!(response.error.is_none());
                    assert_eq!(response.raw_file_info, "null");
                    if let Some((previous, previous_bytes)) = previous {
                        let actual = disk
                            .read_version(
                                volume,
                                volume,
                                &path,
                                &previous.version_id.expect("previous version").to_string(),
                                &ReadOptions {
                                    read_data: true,
                                    ..Default::default()
                                },
                            )
                            .await
                            .expect("read retained or restored version");
                        assert_eq!(actual.version_id, previous.version_id);
                        assert_eq!(actual.data, previous.data);
                        assert_eq!(actual.size, previous.size);
                        if case == "backup" {
                            assert_eq!(tokio::fs::read(&physical).await.expect("restored metadata"), previous_bytes);
                            assert!(!backup.expect("real backup").exists(), "restoration must consume backup");
                        }
                    }
                    if case != "backup" {
                        let missing = disk
                            .read_version(
                                volume,
                                volume,
                                &path,
                                &incoming.version_id.expect("removed version").to_string(),
                                &ReadOptions {
                                    read_data: true,
                                    ..Default::default()
                                },
                            )
                            .await
                            .expect_err("rolled-back version must disappear");
                        assert_eq!(missing, DiskError::FileVersionNotFound);
                    }
                    if case == "fresh" {
                        assert!(!physical.exists(), "fresh rollback removes final metadata");
                    }
                }
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_undo_write_receiver_rejects_missing_volume_with_retained_metadata() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-undo-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("missing undo receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let service = service_for(&env).await;
        let target = service.local_mutation_target();
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("listener local disk");
        for msgpack in [false, true] {
            for case in ["backup", "remaining", "fresh"] {
                let volume = format!("undo-missing-{case}-{msgpack}");
                env.make_bucket(&volume, true).await;
                let path = "nested/object";
                let (incoming, opts, _) = stage_legacy_undo(&disk, &volume, path, case).await;
                let incarnation = env
                    .ecstore
                    .bucket_incarnation_id(&volume)
                    .await
                    .expect("authoritative incarnation");
                disk.delete_volume(&volume, true)
                    .await
                    .expect("remove only receiving physical volume");
                assert_eq!(env.ecstore.bucket_incarnation_id(&volume).await.expect("retained metadata"), incarnation);
                target
                    .reject_legacy_artifact_mutation(&volume)
                    .await
                    .expect("ordinary metadata still classifies");
                let response = authenticated_legacy_delete(
                    &service,
                    signed_legacy_delete_request(&disk_ref, &volume, path, &incoming, &opts, msgpack, false),
                )
                .await
                .expect("missing volume response")
                .into_inner();
                assert!(!response.success);
                assert_eq!(DiskError::from(response.error.expect("missing-volume error")), DiskError::VolumeNotFound);
                assert!(
                    !env.disk_paths[0].join(&volume).exists(),
                    "retained metadata must not recreate volume or object"
                );
            }
        }
    }

    async fn prepare_legacy_delete_rollback(
        disk: &ecstore_disk::DiskStore,
        volume: &str,
        path: &str,
        case: &str,
    ) -> (FileInfo, Option<Vec<u8>>) {
        let object_dir = ecstore_disk::DiskAPI::path(disk.as_ref()).join(volume).join(path);
        let mut fi = inline_rename_file_info(path);
        fi.data = None;
        fi.metadata.clear();
        let original = if case == "marker" {
            fi.deleted = true;
            fi.mark_deleted = true;
            fi.size = 0;
            fi.parts.clear();
            fi.erasure = Default::default();
            assert!(fi.is_canonical_delete_marker(), "fixture must serialize as a delete marker");
            None
        } else {
            let data_dir = Uuid::new_v4();
            fi.data_dir = Some(data_dir);
            let data_path = object_dir.join(data_dir.to_string());
            tokio::fs::create_dir_all(&data_path).await.expect("original data directory");
            tokio::fs::write(data_path.join("part.1"), LEGACY_RENAME_BODY)
                .await
                .expect("original part bytes");
            ecstore_disk::DiskAPI::write_metadata(disk.as_ref(), "", volume, path, fi.clone())
                .await
                .expect("original non-inline version");
            Some(
                tokio::fs::read(object_dir.join("xl.meta"))
                    .await
                    .expect("original metadata bytes"),
            )
        };
        (fi, original)
    }

    async fn stage_legacy_undo_delete(
        disk: &ecstore_disk::DiskStore,
        volume: &str,
        path: &str,
        case: &str,
    ) -> (FileInfo, DeleteOptions, Option<Vec<u8>>) {
        let object_dir = ecstore_disk::DiskAPI::path(disk.as_ref()).join(volume).join(path);
        let rollback = Uuid::new_v4();
        let (fi, original) = prepare_legacy_delete_rollback(disk, volume, path, case).await;
        disk.delete_version(
            volume,
            path,
            fi.clone(),
            case == "marker",
            DeleteOptions {
                old_data_dir: Some(rollback),
                ..Default::default()
            },
        )
        .await
        .expect("real staged delete");
        let rollback_path = object_dir.join(rollback.to_string());
        if case == "marker" {
            assert!(object_dir.join("xl.meta").exists());
            assert!(rollback_path.join("xl.meta.delete-marker.rollback").exists());
            assert!(!rollback_path.join("xl.meta.bkp").exists());
        } else {
            assert!(!object_dir.join("xl.meta").exists());
            assert_eq!(
                tokio::fs::read(rollback_path.join("xl.meta.bkp")).await.expect("real backup"),
                *original.as_ref().expect("old metadata")
            );
            let data_path = object_dir.join(fi.data_dir.expect("data directory").to_string());
            let committed = data_path.join(format!("delete-data.{rollback}"));
            assert!(committed.exists(), "successful staged delete commits its data marker");
            if case == "staged" {
                // Older staged deletes moved the data into the rollback directory.
                tokio::fs::remove_file(committed).await.expect("replace committed layout");
                tokio::fs::rename(&data_path, rollback_path.join(fi.data_dir.expect("data directory").to_string()))
                    .await
                    .expect("real staged data bytes");
                assert!(!data_path.exists());
            }
        }
        (
            fi,
            DeleteOptions {
                undo_write: true,
                undo_delete: true,
                old_data_dir: Some(rollback),
                ..Default::default()
            },
            original,
        )
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_undo_delete_receiver_restores_staged_versions_and_rejects_managed() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-undo-delete-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("undo delete receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("undo-delete-ordinary", true).await;
        env.ecstore
            .clone()
            .make_artifact_bucket("undo-delete-managed", "outputs/")
            .await
            .expect("managed bucket");
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = service
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("listener local disk");
        for msgpack in [false, true] {
            for volume in ["undo-delete-ordinary", ".rustfs.sys", "undo-delete-managed"] {
                for case in ["committed", "staged", "marker"] {
                    let path = if volume == "undo-delete-managed" {
                        format!("outputs/74656e616e74/{}/{}/{case}-{msgpack}", Uuid::new_v4(), Uuid::new_v4())
                    } else {
                        format!("undo-delete/{case}-{msgpack}")
                    };
                    let (fi, opts, original) = stage_legacy_undo_delete(&disk, volume, &path, case).await;
                    let object_dir = env.disk_paths[0].join(volume).join(&path);
                    let rollback = opts.old_data_dir.expect("rollback directory");
                    let rollback_path = object_dir.join(rollback.to_string());
                    let data_name = fi.data_dir.unwrap_or_default().to_string();
                    let preserved_paths = [
                        object_dir.join("xl.meta"),
                        rollback_path.join("xl.meta.bkp"),
                        rollback_path.join("xl.meta.delete-marker.rollback"),
                        object_dir.join(&data_name).join("part.1"),
                        object_dir.join(&data_name).join(format!("delete-data.{rollback}")),
                        rollback_path.join(&data_name).join("part.1"),
                    ];
                    let mut before = Vec::new();
                    for physical in &preserved_paths {
                        before.push(tokio::fs::read(physical).await.ok());
                    }
                    let result = authenticated_legacy_delete(
                        &service,
                        signed_legacy_delete_request(&disk_ref, volume, &path, &fi, &opts, msgpack, false),
                    )
                    .await;
                    if volume == "undo-delete-managed" {
                        assert_eq!(result.expect_err("managed undo delete must reject").code(), tonic::Code::PermissionDenied);
                        for (physical, expected) in preserved_paths.iter().zip(before) {
                            assert_eq!(
                                tokio::fs::read(physical).await.ok(),
                                expected,
                                "managed rollback must remain unchanged: {}",
                                physical.display()
                            );
                        }
                        continue;
                    }
                    let response = result.expect("signed undo delete response").into_inner();
                    assert!(response.success, "volume={volume}, case={case}, msgpack={msgpack}: {:?}", response.error);
                    assert!(response.error.is_none());
                    assert_eq!(response.raw_file_info, "null");
                    let repeated = authenticated_legacy_delete(
                        &service,
                        signed_legacy_delete_request(&disk_ref, volume, &path, &fi, &opts, msgpack, false),
                    )
                    .await
                    .expect("repeated undo response")
                    .into_inner();
                    assert!(repeated.success, "repeated undo: {:?}", repeated.error);
                    assert!(!rollback_path.exists(), "successful undo consumes rollback state");
                    if let Some(original) = original {
                        assert_eq!(tokio::fs::read(object_dir.join("xl.meta")).await.expect("restored metadata"), original);
                        let restored = disk
                            .read_version(
                                volume,
                                volume,
                                &path,
                                &fi.version_id.expect("original version").to_string(),
                                &ReadOptions::default(),
                            )
                            .await
                            .expect("complete restored version");
                        assert_eq!(restored.version_id, fi.version_id);
                        assert_eq!(restored.data_dir, fi.data_dir);
                        assert_eq!(restored.size, fi.size);
                        assert_eq!(
                            tokio::fs::read(object_dir.join(&data_name).join("part.1"))
                                .await
                                .expect("restored data"),
                            LEGACY_RENAME_BODY
                        );
                        assert!(!object_dir.join(&data_name).join(format!("delete-data.{rollback}")).exists());
                    } else {
                        assert!(!object_dir.join("xl.meta").exists(), "marker-only undo removes new metadata");
                    }
                }
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_undo_delete_receiver_rejects_missing_volume_with_retained_authority() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-undo-delete-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("missing undo delete receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let service = service_for(&env).await;
        let target = service.local_mutation_target();
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("listener local disk");
        for msgpack in [false, true] {
            for case in ["committed", "staged", "marker"] {
                let volume = format!("undo-delete-missing-{case}-{msgpack}");
                env.make_bucket(&volume, true).await;
                let path = "nested/object";
                let (fi, opts, _) = stage_legacy_undo_delete(&disk, &volume, path, case).await;
                let incarnation = env
                    .ecstore
                    .bucket_incarnation_id(&volume)
                    .await
                    .expect("authoritative incarnation");
                disk.delete_volume(&volume, true)
                    .await
                    .expect("remove only receiving physical volume");
                assert_eq!(env.ecstore.bucket_incarnation_id(&volume).await.expect("retained authority"), incarnation);
                target
                    .reject_legacy_artifact_mutation(&volume)
                    .await
                    .expect("ordinary authority still classifies");
                let response = authenticated_legacy_delete(
                    &service,
                    signed_legacy_delete_request(&disk_ref, &volume, path, &fi, &opts, msgpack, false),
                )
                .await
                .expect("missing volume response")
                .into_inner();
                assert!(!response.success);
                assert_eq!(DiskError::from(response.error.expect("missing-volume error")), DiskError::VolumeNotFound);
                assert!(
                    !env.disk_paths[0].join(&volume).exists(),
                    "retained authority must not recreate volume or object"
                );
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_staged_delete_receiver_preserves_variants_and_instance_binding() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-staged-delete-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("staged delete receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("staged-delete-ordinary", true).await;
        env.ecstore
            .clone()
            .make_artifact_bucket("staged-delete-managed", "outputs/")
            .await
            .expect("managed bucket");
        let ready = service_for(&env).await;
        let instance = crate::storage::storage_api::bootstrap_instance_ctx();
        assert!(super::super::BootstrapLocalTarget::new(instance.clone()).is_for_store(&env.ecstore));
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = ready
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("listener disk");
        let mut wrong_instance = super::super::make_server_for_context(ready.context.clone());
        wrong_instance.server_ctx = Some(super::super::ServerContextSlot::with_instance_context(
            crate::storage::storage_api::new_instance_ctx(),
        ));
        for bootstrap in [false, true] {
            let slot = super::super::ServerContextSlot::with_instance_context(instance.clone());
            if !bootstrap {
                slot.try_install(ready.context.clone().expect("ready context"))
                    .expect("install this instance");
            }
            let service = super::super::make_server_for_slot(slot);
            assert_eq!(matches!(service.local_mutation_target(), LocalMutationTarget::Bootstrap(_)), bootstrap);
            for msgpack in [false, true] {
                for volume in ["staged-delete-ordinary", ".rustfs.sys", "staged-delete-managed"] {
                    for case in ["remaining", "last", "marker"] {
                        let path = if volume == "staged-delete-managed" {
                            format!("outputs/74656e616e74/{}/{}/{case}-{msgpack}-{bootstrap}", Uuid::new_v4(), Uuid::new_v4())
                        } else {
                            format!("staged-delete/{case}-{msgpack}-{bootstrap}")
                        };
                        let (fi, previous) = if case == "remaining" {
                            stage_legacy_delete(&disk, volume, &path, case).await
                        } else {
                            let (fi, _) = prepare_legacy_delete_rollback(&disk, volume, &path, case).await;
                            (fi, None)
                        };
                        let object_dir = env.disk_paths[0].join(volume).join(&path);
                        let metadata = object_dir.join("xl.meta");
                        let before = tokio::fs::read(&metadata).await.ok();
                        assert_eq!(before.is_none(), case == "marker");
                        let rollback = Uuid::new_v4();
                        let rollback_path = object_dir.join(rollback.to_string());
                        let legacy_opts = DeleteOptions {
                            old_data_dir: Some(rollback),
                            ..Default::default()
                        };
                        if volume == "staged-delete-ordinary" {
                            let original = path_delete_snapshot(&object_dir);
                            let rejected = authenticated_legacy_delete(
                                &service,
                                signed_legacy_delete_request(
                                    &disk_ref,
                                    volume,
                                    &path,
                                    &fi,
                                    &legacy_opts,
                                    msgpack,
                                    case == "marker",
                                ),
                            )
                            .await
                            .expect("obsolete ordinary staging envelope")
                            .into_inner();
                            assert!(!rejected.success);
                            assert_eq!(
                                DiskError::from(rejected.error.expect("obsolete transaction")),
                                DiskError::UnsupportedDisk
                            );
                            assert_eq!(
                                path_delete_snapshot(&object_dir),
                                original,
                                "obsolete transaction changes no physical bytes"
                            );
                            assert!(!rollback_path.exists());
                        }
                        let opts = if volume == ".rustfs.sys" {
                            legacy_opts
                        } else {
                            DeleteOptions::default()
                        };
                        if volume == ".rustfs.sys" && case == "last" {
                            let rejected = authenticated_legacy_delete(
                                &wrong_instance,
                                signed_legacy_delete_request(&disk_ref, volume, &path, &fi, &opts, msgpack, false),
                            )
                            .await
                            .expect("wrong-instance response")
                            .into_inner();
                            assert!(!rejected.success, "a listener must not fall back to its ready context or ambient disks");
                            assert!(rejected.error.is_some());
                            assert_eq!(tokio::fs::read(&metadata).await.ok(), before);
                            assert!(!rollback_path.exists());
                        }
                        let result = authenticated_legacy_delete(
                            &service,
                            signed_legacy_delete_request(&disk_ref, volume, &path, &fi, &opts, msgpack, case == "marker"),
                        )
                        .await;
                        if bootstrap && volume != ".rustfs.sys" {
                            let response = result.expect("bootstrap admission response").into_inner();
                            assert!(!response.success);
                            assert_eq!(DiskError::from(response.error.expect("bootstrap error")), DiskError::FileAccessDenied);
                        } else if volume == "staged-delete-managed" {
                            assert_eq!(result.expect_err("managed staging rejects").code(), tonic::Code::PermissionDenied);
                        } else if volume == "staged-delete-ordinary" {
                            let response = result.expect("ordinary nontransactional delete response").into_inner();
                            assert!(response.success, "ordinary current delete succeeds: {:?}", response.error);
                            assert!(response.error.is_none());
                            assert_eq!(response.raw_file_info, "null");
                            let current = disk
                                .read_version(
                                    volume,
                                    volume,
                                    &path,
                                    &fi.version_id.expect("requested version").to_string(),
                                    &ReadOptions::default(),
                                )
                                .await;
                            if case == "marker" {
                                let marker = current.expect("ordinary marker is durably created");
                                assert!(marker.is_canonical_delete_marker());
                                assert_eq!(marker.version_id, fi.version_id);
                            } else {
                                assert_eq!(
                                    current.expect_err("ordinary selected version is removed"),
                                    DiskError::FileVersionNotFound
                                );
                                if let Some((previous, _)) = previous {
                                    let retained = disk
                                        .read_version(
                                            volume,
                                            volume,
                                            &path,
                                            &previous.version_id.expect("previous version").to_string(),
                                            &ReadOptions {
                                                read_data: true,
                                                ..Default::default()
                                            },
                                        )
                                        .await
                                        .expect("ordinary delete retains the other version");
                                    assert_eq!(retained.version_id, previous.version_id);
                                    assert_eq!(retained.data, previous.data);
                                } else {
                                    assert!(!metadata.exists(), "ordinary final version metadata is removed");
                                }
                                if let Some(data_dir) = fi.data_dir {
                                    assert!(
                                        !object_dir.join(data_dir.to_string()).exists(),
                                        "ordinary final version parts are removed"
                                    );
                                }
                            }
                            assert!(!rollback_path.exists(), "current direct delete does not stage legacy rollback");
                            continue;
                        } else {
                            let response = result.expect("signed staged delete response").into_inner();
                            assert!(
                                response.success,
                                "volume={volume}, case={case}, bootstrap={bootstrap}, msgpack={msgpack}: {:?}",
                                response.error
                            );
                            assert!(response.error.is_none());
                            assert_eq!(response.raw_file_info, "null");
                            if case == "marker" {
                                assert!(rollback_path.join("xl.meta.delete-marker.rollback").exists());
                                assert!(!rollback_path.join("xl.meta.bkp").exists());
                                let marker = disk
                                    .read_version(
                                        volume,
                                        volume,
                                        &path,
                                        &fi.version_id.expect("marker version").to_string(),
                                        &ReadOptions::default(),
                                    )
                                    .await
                                    .expect("persisted forced marker");
                                assert!(marker.is_canonical_delete_marker());
                                assert_eq!(marker.version_id, fi.version_id);
                            } else {
                                assert_eq!(
                                    tokio::fs::read(rollback_path.join("xl.meta.bkp"))
                                        .await
                                        .expect("staged exact backup"),
                                    *before.as_ref().expect("old metadata")
                                );
                                assert_eq!(
                                    disk.read_version(
                                        volume,
                                        volume,
                                        &path,
                                        &fi.version_id.expect("deleted version").to_string(),
                                        &ReadOptions::default(),
                                    )
                                    .await
                                    .expect_err("forward staging removes the requested version"),
                                    DiskError::FileVersionNotFound
                                );
                                if let Some((previous, _)) = previous {
                                    let retained = disk
                                        .read_version(
                                            volume,
                                            volume,
                                            &path,
                                            &previous.version_id.expect("previous version").to_string(),
                                            &ReadOptions {
                                                read_data: true,
                                                ..Default::default()
                                            },
                                        )
                                        .await
                                        .expect("remaining version");
                                    assert_eq!(retained.data, previous.data);
                                    assert_eq!(retained.version_id, previous.version_id);
                                } else {
                                    assert!(!metadata.exists(), "last version removed");
                                }
                            }
                            let undo = DeleteOptions {
                                undo_write: true,
                                undo_delete: true,
                                ..opts
                            };
                            let response = authenticated_legacy_delete(
                                &service,
                                signed_legacy_delete_request(&disk_ref, volume, &path, &fi, &undo, msgpack, false),
                            )
                            .await
                            .expect("signed undo response")
                            .into_inner();
                            assert!(response.success, "staged delete undo: {:?}", response.error);
                            assert!(!rollback_path.exists(), "undo consumes rollback state");
                            if case != "marker" {
                                let restored = disk
                                    .read_version(
                                        volume,
                                        volume,
                                        &path,
                                        &fi.version_id.expect("original version").to_string(),
                                        &ReadOptions {
                                            read_data: true,
                                            ..Default::default()
                                        },
                                    )
                                    .await
                                    .expect("restored exact version");
                                assert_eq!(restored.version_id, fi.version_id);
                                assert_eq!(restored.size, fi.size);
                                assert_eq!(restored.data_dir, fi.data_dir);
                                if fi.data.is_some() {
                                    assert_eq!(restored.data, fi.data);
                                }
                            }
                        }
                        assert_eq!(
                            tokio::fs::read(&metadata).await.ok(),
                            before,
                            "rejected or undone staging preserves metadata bytes"
                        );
                        assert!(!rollback_path.exists());
                        if let Some(data_dir) = fi.data_dir {
                            let data_path = object_dir.join(data_dir.to_string());
                            assert_eq!(
                                tokio::fs::read(data_path.join("part.1"))
                                    .await
                                    .expect("original part retained"),
                                LEGACY_RENAME_BODY
                            );
                            for prefix in ["reserve-delete-data.", "delete-data."] {
                                assert!(!data_path.join(format!("{prefix}{rollback}")).exists());
                            }
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_staged_delete_receiver_rejects_missing_volume_with_retained_authority() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-staged-delete-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("missing staged delete receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let service = service_for(&env).await;
        let target = service.local_mutation_target();
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("listener local disk");
        for msgpack in [false, true] {
            for case in ["remaining", "last", "marker"] {
                let volume = format!("staged-delete-missing-{case}-{msgpack}");
                env.make_bucket(&volume, true).await;
                let path = "nested/object";
                let fi = if case == "remaining" {
                    stage_legacy_delete(&disk, &volume, path, case).await.0
                } else {
                    prepare_legacy_delete_rollback(&disk, &volume, path, case).await.0
                };
                let incarnation = env
                    .ecstore
                    .bucket_incarnation_id(&volume)
                    .await
                    .expect("authoritative incarnation");
                disk.delete_volume(&volume, true).await.expect("remove only receiving volume");
                assert_eq!(env.ecstore.bucket_incarnation_id(&volume).await.expect("retained authority"), incarnation);
                target
                    .reject_legacy_artifact_mutation(&volume)
                    .await
                    .expect("ordinary authority still classifies");
                let opts = DeleteOptions::default();
                let response = authenticated_legacy_delete(
                    &service,
                    signed_legacy_delete_request(&disk_ref, &volume, path, &fi, &opts, msgpack, case == "marker"),
                )
                .await
                .expect("missing-volume response")
                .into_inner();
                assert!(!response.success);
                assert_eq!(DiskError::from(response.error.expect("missing-volume error")), DiskError::VolumeNotFound);
                assert!(
                    !env.disk_paths[0].join(&volume).exists(),
                    "retained authority must not recreate physical parents"
                );
            }
        }
    }

    async fn stage_legacy_delete(
        disk: &ecstore_disk::DiskStore,
        volume: &str,
        path: &str,
        case: &str,
    ) -> (FileInfo, Option<(FileInfo, Vec<u8>)>) {
        if case == "marker" {
            return (
                FileInfo {
                    volume: volume.to_owned(),
                    name: path.to_owned(),
                    version_id: Some(Uuid::new_v4()),
                    deleted: true,
                    mod_time: Some(time::OffsetDateTime::now_utc()),
                    ..Default::default()
                },
                None,
            );
        }
        let (incoming, opts, previous) = stage_legacy_undo(disk, volume, path, case).await;
        assert!(opts.old_data_dir.is_none(), "ordinary deletion must not stage a rollback");
        (incoming, previous)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_delete_version_receiver_preserves_versions_and_rejects_managed() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-delete-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("delete receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("delete-ordinary", true).await;
        env.ecstore
            .clone()
            .make_artifact_bucket("delete-managed", "outputs/")
            .await
            .expect("managed bucket");
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = service
            .local_mutation_target()
            .find_mutation_disk(&disk_ref)
            .await
            .expect("listener local disk");
        for msgpack in [false, true] {
            for volume in ["delete-ordinary", ".rustfs.sys", "delete-managed"] {
                for case in ["remaining", "fresh", "marker"] {
                    let path = if volume == "delete-managed" {
                        format!("outputs/74656e616e74/{}/{}/{case}-{msgpack}", Uuid::new_v4(), Uuid::new_v4())
                    } else {
                        format!("delete/{case}-{msgpack}")
                    };
                    let (incoming, previous) = stage_legacy_delete(&disk, volume, &path, case).await;
                    let physical = env.disk_paths[0].join(volume).join(&path).join("xl.meta");
                    let before = if case == "marker" {
                        assert!(!physical.parent().expect("object directory").exists());
                        None
                    } else {
                        Some(tokio::fs::read(&physical).await.expect("published metadata"))
                    };
                    let result = authenticated_legacy_delete(
                        &service,
                        signed_legacy_delete_request(
                            &disk_ref,
                            volume,
                            &path,
                            &incoming,
                            &DeleteOptions::default(),
                            msgpack,
                            case == "marker",
                        ),
                    )
                    .await;
                    if volume == "delete-managed" {
                        assert_eq!(result.expect_err("managed deletion must reject").code(), tonic::Code::PermissionDenied);
                        if let Some(before) = before {
                            assert_eq!(tokio::fs::read(&physical).await.expect("managed metadata retained"), before);
                        } else {
                            assert!(
                                !physical.parent().expect("managed object directory").exists(),
                                "rejected marker must not create object parents"
                            );
                        }
                        continue;
                    }
                    let response = result.expect("signed deletion response").into_inner();
                    assert!(response.success, "volume={volume}, case={case}, msgpack={msgpack}: {:?}", response.error);
                    assert!(response.error.is_none());
                    assert_eq!(response.raw_file_info, "null");
                    if case == "marker" {
                        let raw = tokio::fs::read(&physical).await.expect("new marker metadata");
                        let meta = rustfs_filemeta::FileMeta::load(&raw).expect("decode physical marker");
                        assert_eq!(meta.versions.len(), 1);
                        let marker = meta
                            .into_fileinfo(
                                volume,
                                &path,
                                &incoming.version_id.expect("marker version").to_string(),
                                true,
                                false,
                                true,
                            )
                            .expect("read complete persisted marker");
                        assert!(marker.deleted);
                        assert_eq!(marker.version_id, incoming.version_id);
                        assert_eq!(marker.mod_time, incoming.mod_time);
                        continue;
                    }
                    if let Some((previous, _)) = previous {
                        let retained = disk
                            .read_version(
                                volume,
                                volume,
                                &path,
                                &previous.version_id.expect("previous version").to_string(),
                                &ReadOptions {
                                    read_data: true,
                                    ..Default::default()
                                },
                            )
                            .await
                            .expect("read retained version");
                        assert_eq!(retained.version_id, previous.version_id);
                        assert_eq!(retained.data, previous.data);
                        assert_eq!(retained.size, previous.size);
                    }
                    let missing = disk
                        .read_version(
                            volume,
                            volume,
                            &path,
                            &incoming.version_id.expect("removed version").to_string(),
                            &ReadOptions {
                                read_data: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect_err("deleted version must disappear");
                    assert_eq!(missing, DiskError::FileVersionNotFound);
                    if case == "fresh" {
                        assert!(!physical.exists(), "last-version deletion removes final metadata");
                    }
                }
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_delete_version_receiver_rejects_missing_volume_with_retained_metadata() {
        let _ = rustfs_credentials::set_global_rpc_secret("legacy-delete-receiver-test-secret".to_owned());
        let root = tempfile::tempdir().expect("missing delete receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let service = service_for(&env).await;
        let target = service.local_mutation_target();
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("listener local disk");
        for msgpack in [false, true] {
            for case in ["remaining", "fresh", "marker"] {
                let volume = format!("delete-missing-{case}-{msgpack}");
                env.make_bucket(&volume, true).await;
                let path = "nested/object";
                let (incoming, _) = stage_legacy_delete(&disk, &volume, path, case).await;
                let incarnation = env
                    .ecstore
                    .bucket_incarnation_id(&volume)
                    .await
                    .expect("authoritative incarnation");
                disk.delete_volume(&volume, true)
                    .await
                    .expect("remove only receiving physical volume");
                assert_eq!(env.ecstore.bucket_incarnation_id(&volume).await.expect("retained metadata"), incarnation);
                target
                    .reject_legacy_artifact_mutation(&volume)
                    .await
                    .expect("ordinary metadata still classifies");
                let response = authenticated_legacy_delete(
                    &service,
                    signed_legacy_delete_request(
                        &disk_ref,
                        &volume,
                        path,
                        &incoming,
                        &DeleteOptions::default(),
                        msgpack,
                        case == "marker",
                    ),
                )
                .await
                .expect("missing volume response")
                .into_inner();
                assert!(!response.success);
                assert_eq!(response.raw_file_info, "");
                assert_eq!(DiskError::from(response.error.expect("missing-volume error")), DiskError::VolumeNotFound);
                assert!(
                    !env.disk_paths[0].join(&volume).exists(),
                    "retained metadata must not allow deletion or marker publication to recreate the volume"
                );
            }
        }
    }

    fn metadata_request(disk: &str, volume: &str, path: &str, version: Uuid) -> Request<WriteMetadataRequest> {
        let file_info = FileInfo {
            volume: volume.to_owned(),
            name: path.to_owned(),
            version_id: Some(version),
            deleted: true,
            mod_time: Some(time::OffsetDateTime::now_utc()),
            ..Default::default()
        };
        let mut request = Request::new(WriteMetadataRequest {
            disk: disk.to_owned(),
            volume: volume.to_owned(),
            path: path.to_owned(),
            file_info: serde_json::to_string(&file_info).expect("metadata request"),
            ..Default::default()
        });
        let body = rustfs_protos::canonical_write_metadata_request_body(request.get_ref()).expect("canonical metadata");
        set_tonic_canonical_body_digest(&mut request, &body).expect("metadata digest");
        mark_v2_authenticated(&mut request);
        request
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_metadata_receiver_rejects_missing_volume_with_retained_metadata() {
        let root = tempfile::tempdir().expect("missing metadata volume root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let bucket = "metadata-missing-volume";
        env.make_bucket(bucket, false).await;
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let target = service.local_mutation_target();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("listener local disk");
        disk.delete_volume(bucket, true).await.expect("remove only target volume");
        target
            .reject_legacy_artifact_mutation(bucket)
            .await
            .expect("ordinary metadata still exists");

        let response = service
            .handle_write_metadata(metadata_request(&disk_ref, bucket, "nested/object", Uuid::new_v4()))
            .await
            .expect("metadata response")
            .into_inner();
        assert!(!response.success, "LEGACY_METADATA_MISSING_VOLUME_REJECTED");
        assert_eq!(DiskError::from(response.error.expect("missing volume error")), DiskError::VolumeNotFound);
        assert!(!env.disk_paths[0].join(bucket).exists(), "metadata must not recreate the deleted volume");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_metadata_receiver_preserves_versions_and_rejects_managed_writes() {
        let root = tempfile::tempdir().expect("metadata receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("metadata-ordinary", false).await;
        env.ecstore
            .clone()
            .make_artifact_bucket("metadata-managed", "outputs/")
            .await
            .expect("managed bucket");
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();

        let denied = service
            .handle_write_metadata(metadata_request(&disk_ref, "metadata-managed", "outputs/object", Uuid::new_v4()))
            .await
            .expect_err("managed metadata requires artifact admission");
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);
        assert!(!env.disk_paths[0].join("metadata-managed/outputs/object").exists());

        for volume in ["metadata-ordinary", ".rustfs.sys"] {
            let path = "legacy-metadata/nested/object";
            let versions = [Uuid::new_v4(), Uuid::new_v4()];
            for version in versions {
                let response = service
                    .handle_write_metadata(metadata_request(&disk_ref, volume, path, version))
                    .await
                    .expect("metadata response")
                    .into_inner();
                assert!(response.success, "{volume}: {:?}", response.error);
            }
            let raw = tokio::fs::read(env.disk_paths[0].join(volume).join(path).join("xl.meta"))
                .await
                .expect("physical metadata");
            let meta = rustfs_filemeta::FileMeta::load(&raw).expect("decode physical metadata");
            for version in versions {
                let restored = meta
                    .into_fileinfo(volume, path, &version.to_string(), true, false, true)
                    .expect("both versions survive metadata rewrite");
                assert_eq!(restored.version_id, Some(version));
                assert!(restored.deleted);
            }
        }
    }

    fn cas_request(
        disk: &str,
        volume: &str,
        path: &str,
        expected: Option<Bytes>,
        replacement: Option<Bytes>,
    ) -> Request<CompareAndUpdateFileRequest> {
        let mut request = Request::new(CompareAndUpdateFileRequest {
            disk: disk.to_owned(),
            volume: volume.to_owned(),
            path: path.to_owned(),
            expected,
            replacement,
        });
        let body = rustfs_protos::canonical_compare_and_update_file_request_body(request.get_ref()).expect("canonical CAS");
        set_tonic_canonical_body_digest(&mut request, &body).expect("CAS digest");
        mark_v2_authenticated(&mut request);
        request
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_file_receiver_rejects_missing_volume_with_retained_metadata() {
        let root = tempfile::tempdir().expect("missing volume root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        let bucket = "legacy-missing-volume";
        env.make_bucket(bucket, false).await;
        let service = service_for(&env).await;
        let disk_ref = env.disk_paths[0].to_string_lossy().into_owned();
        let target = service.local_mutation_target();
        let disk = target.find_mutation_disk(&disk_ref).await.expect("listener local disk");
        disk.delete_volume(bucket, true).await.expect("remove only target volume");
        target
            .reject_legacy_artifact_mutation(bucket)
            .await
            .expect("ordinary metadata remains authoritative");
        assert!(!env.disk_paths[0].join(bucket).exists());

        let write = service
            .handle_write_all(write_request(&disk_ref, bucket, "nested/object"))
            .await
            .expect("write response")
            .into_inner();
        assert!(!write.success, "ARTIFACT_LEGACY_MISSING_VOLUME_REJECTED");
        assert!(!env.disk_paths[0].join(bucket).exists(), "ARTIFACT_LEGACY_MISSING_VOLUME_NOT_RECREATED");
        let cas = service
            .handle_compare_and_update_file(cas_request(&disk_ref, bucket, "conditional", None, Some(Bytes::from_static(b"new"))))
            .await
            .expect("CAS response")
            .into_inner();
        assert!(!cas.success, "missing target volume must also reject CAS");
        assert!(!env.disk_paths[0].join(bucket).exists(), "CAS cannot recreate the missing volume");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_file_receiver_cas_preserves_ordinary_and_internal_semantics() {
        let root = tempfile::tempdir().expect("CAS receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("legacy-cas-ordinary", false).await;
        env.ecstore
            .clone()
            .make_artifact_bucket("legacy-cas-managed", "outputs/")
            .await
            .expect("managed bucket");
        let service = service_for(&env).await;
        let disk = env.disk_paths[0].to_string_lossy().into_owned();
        let denied = service
            .handle_compare_and_update_file(cas_request(
                &disk,
                "legacy-cas-managed",
                "blocked",
                None,
                Some(Bytes::from_static(b"new")),
            ))
            .await
            .expect_err("managed CAS denied");
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);
        assert!(!env.disk_paths[0].join("legacy-cas-managed/blocked").exists());

        for volume in ["legacy-cas-ordinary", ".rustfs.sys"] {
            for (expected, replacement, outcome) in [
                (
                    None,
                    Some(Bytes::from_static(b"first")),
                    CompareAndUpdateFileOutcome::CompareAndUpdateFileUpdated,
                ),
                (
                    Some(Bytes::from_static(b"wrong")),
                    Some(Bytes::from_static(b"wrong")),
                    CompareAndUpdateFileOutcome::CompareAndUpdateFileMismatch,
                ),
                (
                    Some(Bytes::from_static(b"first")),
                    Some(Bytes::from_static(b"second")),
                    CompareAndUpdateFileOutcome::CompareAndUpdateFileUpdated,
                ),
                (
                    Some(Bytes::from_static(b"second")),
                    None,
                    CompareAndUpdateFileOutcome::CompareAndUpdateFileUpdated,
                ),
            ] {
                let response = service
                    .handle_compare_and_update_file(cas_request(&disk, volume, "conditional", expected, replacement.clone()))
                    .await
                    .expect("CAS response")
                    .into_inner();
                assert!(response.success, "volume={volume}: CAS must retain ordinary behavior");
                assert_eq!(response.outcome, outcome as i32);
                let file = env.disk_paths[0].join(volume).join("conditional");
                let expected = if outcome == CompareAndUpdateFileOutcome::CompareAndUpdateFileMismatch {
                    Some(Bytes::from_static(b"first"))
                } else {
                    replacement
                };
                if let Some(expected) = expected {
                    assert_eq!(tokio::fs::read(file).await.expect("committed bytes"), expected);
                } else {
                    assert!(!file.exists(), "conditional deletion must remove the actual file");
                }
            }
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_artifact_receiver_rejects_managed_but_persists_ordinary_and_internal_writes() {
        let root = tempfile::tempdir().expect("receiver root");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(root.path())
            .build()
            .await;
        env.make_bucket("legacy-ordinary", false).await;
        env.ecstore
            .clone()
            .make_artifact_bucket("legacy-managed", "outputs/")
            .await
            .expect("managed bucket");
        let service = service_for(&env).await;
        let disk = env.disk_paths[0].to_string_lossy().into_owned();
        let denied = service
            .handle_write_all(write_request(&disk, "legacy-managed", "blocked"))
            .await
            .expect_err("legacy managed write denied");
        assert_eq!(denied.code(), tonic::Code::PermissionDenied, "ARTIFACT_LEGACY_RPC_ADMISSION");
        assert!(!env.disk_paths[0].join("legacy-managed/blocked").exists(), "ARTIFACT_LEGACY_RPC_NO_WRITE");
        let ordinary = service
            .handle_write_all(write_request(&disk, "legacy-ordinary", "permitted"))
            .await
            .expect("ordinary receiver")
            .into_inner();
        assert!(ordinary.success, "ARTIFACT_LEGACY_RPC_ORDINARY_COMPAT");
        assert_eq!(
            tokio::fs::read(env.disk_paths[0].join("legacy-ordinary/permitted"))
                .await
                .expect("ordinary bytes"),
            b"legacy receiver bytes"
        );
        let internal = service
            .handle_write_all(write_request(&disk, ".rustfs.sys", "legacy-source"))
            .await
            .expect("internal receiver")
            .into_inner();
        assert!(internal.success, "ARTIFACT_LEGACY_RPC_INTERNAL_COMPAT");
        let internal_volume = ".rustfs.sys/tmp/legacy-created";
        assert!(!env.disk_paths[0].join(internal_volume).exists());
        let created = service
            .handle_write_all(write_request(&disk, internal_volume, "nested/file"))
            .await
            .expect("internal on-demand directory response")
            .into_inner();
        assert!(created.success, "internal volumes retain on-demand directory creation");
        assert_eq!(
            tokio::fs::read(env.disk_paths[0].join(internal_volume).join("nested/file"))
                .await
                .expect("internal on-demand bytes"),
            b"legacy receiver bytes"
        );
        let mut rename = Request::new(RenameFileRequest {
            disk,
            src_volume: ".rustfs.sys".into(),
            src_path: "legacy-source".into(),
            dst_volume: "legacy-managed".into(),
            dst_path: "renamed".into(),
            durable: false,
        });
        let body = rustfs_protos::canonical_rename_file_request_body(rename.get_ref()).expect("canonical rename");
        set_tonic_canonical_body_digest(&mut rename, &body).expect("rename digest");
        mark_v2_authenticated(&mut rename);
        let denied = service
            .handle_rename_file(rename)
            .await
            .expect_err("internal staging cannot bypass managed admission");
        assert_eq!(denied.code(), tonic::Code::PermissionDenied, "ARTIFACT_LEGACY_RENAME_ADMISSION");
        assert!(
            !env.disk_paths[0].join("legacy-managed/renamed").exists(),
            "ARTIFACT_LEGACY_RENAME_NO_WRITE"
        );
        assert_eq!(
            tokio::fs::read(env.disk_paths[0].join(".rustfs.sys/legacy-source"))
                .await
                .expect("staged source remains"),
            b"legacy receiver bytes"
        );
        // Deleting an empty legacy volume does not publish a version and must
        // not require a metadata migration under the caller's namespace lock.
        tokio::fs::create_dir(env.disk_paths[0].join("legacy-empty"))
            .await
            .expect("old empty volume");
        let mut deletion = Request::new(DeleteVolumeRequest {
            disk: env.disk_paths[0].to_string_lossy().into_owned(),
            volume: "legacy-empty".into(),
            force: false,
        });
        let canonical = rustfs_protos::canonical_delete_volume_request_body(deletion.get_ref()).expect("canonical delete");
        set_tonic_canonical_body_digest(&mut deletion, &canonical).expect("delete digest");
        mark_v2_authenticated(&mut deletion);
        assert!(
            service
                .handle_delete_volume(deletion)
                .await
                .expect("legacy deletion")
                .into_inner()
                .success,
            "ARTIFACT_LEGACY_METADATALESS_VOLUME_DELETE"
        );
        assert!(!env.disk_paths[0].join("legacy-empty").exists());
    }

    #[tokio::test]
    async fn legacy_artifact_unbound_receiver_only_allows_canonical_internal_volumes() {
        let target = LocalMutationTarget::Unbound;
        assert!(target.reject_legacy_artifact_mutation(".rustfs.sys/tmp").await.is_ok());
        assert!(target.reject_legacy_artifact_mutation("legacy-ordinary").await.is_err());
        assert!(
            target
                .reject_legacy_artifact_mutation(".rustfs.sys/../legacy-managed")
                .await
                .is_err()
        );
    }
}
