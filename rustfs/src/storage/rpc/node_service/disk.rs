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
use crate::storage::storage_api::rpc_consumer::node_service::{
    BatchReadVersionReq, BatchReadVersionResp, ConditionalFileUpdate, DeleteOptions, DiskError, DiskInfoOptions,
    FileInfoVersions, ReadMultipleReq, ReadMultipleResp, ReadOptions, StorageDiskRpcExt as _, UpdateMetadataOpts,
    validate_batch_read_version_item_count,
};
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use crate::storage::storage_api::{
    LegacyRenameData, PartTransactionAction, RenameDataResp, SnapshotLeaseToken, verify_tonic_mutation_body_digest,
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

    async fn undo_local_write(
        &self,
        disk_ref: &str,
        volume: &str,
        path: &str,
        fi: FileInfo,
        opts: DeleteOptions,
    ) -> Result<(), DiskError> {
        match self {
            Self::Ready(store) => store.undo_local_write(disk_ref, volume, path, fi, opts).await,
            Self::Bootstrap(target) => target.undo_local_write(disk_ref, volume, path, fi, opts).await,
            Self::Unbound => Err(DiskError::other("target disk instance is unavailable")),
        }
    }
}

/// Initial capacity hint (bytes) for typical small msgpack requests and responses.
const MSGPACK_ENCODE_CAPACITY_HINT: usize = 512;
const FILE_INFO_MSGPACK_ENCODE_CAPACITY_HINT: usize = 1024;
const SNAPSHOT_LEASE_PROTOCOL_VERSION: u32 = 1;

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
    pub(super) async fn handle_acquire_snapshot_lease(
        &self,
        request: Request<SnapshotLeaseRequest>,
    ) -> Result<Response<SnapshotLeaseResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_snapshot_lease_request_body(request.get_ref()),
            "acquire_snapshot_lease",
        )?;
        let request = request.into_inner();
        let result = match self.find_disk(&request.disk).await {
            Some(disk) => disk.acquire_snapshot_lease(&request.volume, &request.path).await,
            None => Err(DiskError::other("cannot find disk")),
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
    ) -> Result<Response<SnapshotLeaseMutationResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_snapshot_lease_release_request_body(request.get_ref()),
            "release_snapshot_lease",
        )?;
        let request = request.into_inner();
        let token = if request.token.as_ref() == SnapshotLeaseToken::revoke_all().as_bytes() {
            SnapshotLeaseToken::revoke_all()
        } else {
            SnapshotLeaseToken::from_slice(&request.token).map_err(|_| Status::invalid_argument("invalid lease token"))?
        };
        let Some(disk) = self.find_disk(&request.disk).await else {
            return Ok(Response::new(SnapshotLeaseMutationResponse {
                success: false,
                error: Some(DiskError::other("cannot find disk").into()),
            }));
        };
        match disk.release_snapshot_lease(&request.volume, &request.path, token).await {
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
            target.reject_legacy_artifact_mutation(&request.volume).await?;
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

            let (errors, item_errors) =
                encode_delete_versions_errors(disk.delete_versions(&request.volume, versions, opts).await);

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
            target.reject_legacy_artifact_mutation(&request.volume).await?;
            if request.force_del_marker {
                Err(DiskError::other("undo_write cannot force a delete marker"))
            } else {
                target
                    .undo_local_write(&request.disk, &request.volume, &request.path, file_info, opts)
                    .await
            }
        } else if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            target.reject_legacy_artifact_mutation(&request.volume).await?;
            disk.delete_version(&request.volume, &request.path, file_info, request.force_del_marker, opts)
                .await
        } else {
            Err(DiskError::other("cannot find disk"))
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

            match disk.update_metadata(&request.volume, &request.path, file_info, &opts).await {
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
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            target.reject_legacy_artifact_mutation(&request.volume).await?;
            match disk.delete_paths(&request.volume, &request.paths).await {
                Ok(_) => Ok(Response::new(DeletePathsResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(DeletePathsResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DeletePathsResponse {
                success: false,
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
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
        if let Some(disk) = self.find_disk(&request.disk).await {
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
        if let Some(disk) = self.find_disk(&request.disk).await {
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
    ) -> Result<Response<RenameDataResponse>, Status> {
        if !request.get_ref().scanner_publication_lease_token.is_empty() || !request.get_ref().bucket_incarnation_id.is_empty() {
            let has_body_digest = request
                .metadata()
                .get("x-rustfs-content-sha256")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value != "UNSIGNED-PAYLOAD");
            if !has_body_digest {
                return Err(Status::permission_denied("scanner publication lease rename requires a body-bound digest"));
            }
        }
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_rename_data_request_body(request.get_ref()),
            "rename_data",
        )?;
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
                return Ok(Response::new(RenameDataResponse {
                    success: false,
                    rename_data_resp: String::new(),
                    rename_data_resp_bin: Vec::new().into(),
                    error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
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
        #[cfg(feature = "e2e-test-hooks")]
        if let Some(mut observation) = observation {
            observation["ok"] = serde_json::json!(result.is_ok());
            observation["error"] = serde_json::json!(result.as_ref().err().map(ToString::to_string));
            let line = format!("RUSTFS_E2E_STARTUP_CAS {observation}\n");
            let _ = std::io::Write::write_all(&mut std::io::stderr().lock(), line.as_bytes());
        }
        match result {
            Ok(rename_data_resp) => match encode_rename_data_response_payloads(&rename_data_resp, request_decoded_from_msgpack) {
                Ok((rename_data_resp, rename_data_resp_bin)) => Ok(Response::new(RenameDataResponse {
                    success: true,
                    rename_data_resp,
                    rename_data_resp_bin: rename_data_resp_bin.into(),
                    error: None,
                })),
                Err(err) => Ok(Response::new(RenameDataResponse {
                    success: false,
                    rename_data_resp: String::new(),
                    rename_data_resp_bin: Vec::new().into(),
                    error: Some(err.into()),
                })),
            },
            Err(err) => Ok(Response::new(RenameDataResponse {
                success: false,
                rename_data_resp: String::new(),
                rename_data_resp_bin: Vec::new().into(),
                error: Some(err.into()),
            })),
        }
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
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            target.reject_legacy_artifact_mutation(&request.src_volume).await?;
            target.reject_legacy_artifact_mutation(&request.dst_volume).await?;
            let result = if request.durable {
                disk.rename_file_durable(&request.src_volume, &request.src_path, &request.dst_volume, &request.dst_path)
                    .await
            } else {
                disk.rename_file(&request.src_volume, &request.src_path, &request.dst_volume, &request.dst_path)
                    .await
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
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            target.reject_legacy_artifact_mutation(&request.src_volume).await?;
            target.reject_legacy_artifact_mutation(&request.dst_volume).await?;
            match disk
                .rename_part(
                    &request.src_volume,
                    &request.src_path,
                    &request.dst_volume,
                    &request.dst_path,
                    request.meta,
                )
                .await
            {
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
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            target.reject_legacy_artifact_mutation(&request.src_volume).await?;
            target.reject_legacy_artifact_mutation(&request.dst_volume).await?;
            match disk
                .prepare_part_transaction(
                    &request.src_volume,
                    &request.src_path,
                    &request.dst_volume,
                    &request.dst_path,
                    request.meta,
                )
                .await
            {
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
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            target.reject_legacy_artifact_mutation(&request.volume).await?;
            let action = if request.rollback {
                PartTransactionAction::Rollback
            } else {
                PartTransactionAction::Commit
            };
            match disk.settle_part_transaction(&request.volume, &request.path, action).await {
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
        if let Some(disk) = target.find_mutation_disk(&request.disk).await {
            target.reject_legacy_artifact_mutation(&request.volume).await?;
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
            // The target-side guard spans the complete delete operation. A
            // lease expiry or movement transition cannot occur between this
            // validation and the disk delete linearization point.
            let scanner_publication_lease_guard: Option<Arc<dyn Send + Sync>> =
                if let Some(token) = scanner_publication_lease_token {
                    let Some(store) = self.resolve_object_store() else {
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
            match disk
                .delete_with_scanner_publication_lease_and_guard(
                    &request.volume,
                    &request.path,
                    options,
                    scanner_publication_lease_token,
                    scanner_publication_lease_guard,
                )
                .await
            {
                Ok(_) => Ok(Response::new(DeleteResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(DeleteResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DeleteResponse {
                success: false,
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
        }
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
        compat_response_json, decode_msgpack_or_json, decode_rename_data_request_file_info,
        encode_batch_read_version_response_payloads, encode_delete_versions_errors, encode_file_info_msgpack, encode_msgpack,
        encode_msgpack_named, encode_read_multiple_response_payloads, encode_rename_data_response_payloads,
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

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct SamplePayload {
        name: String,
        count: u32,
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

    const LEGACY_RENAME_AUDIENCE: &str = "127.0.0.1:24800";
    const LEGACY_RENAME_RPC_PATH: &str = "/node_service.NodeService/RenameData";
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
        use crate::storage::storage_api::ecstore_rpc as rpc;
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
        set_tonic_canonical_body_digest(&mut request, &body).expect("rename digest");
        let digest = request
            .metadata()
            .get("x-rustfs-content-sha256")
            .expect("digest metadata")
            .to_str()
            .expect("digest text");
        let mut headers =
            rpc::gen_tonic_signature_headers(LEGACY_RENAME_AUDIENCE, "node_service.NodeService", "RenameData", Some(digest))
                .expect("real HMAC signature");
        let challenge = Uuid::new_v4();
        let epoch_proof = rpc::tonic_boot_epoch_response_headers(LEGACY_RENAME_AUDIENCE, challenge).expect("signed epoch proof");
        let epoch =
            rpc::verify_tonic_boot_epoch_response(LEGACY_RENAME_AUDIENCE, challenge, &epoch_proof).expect("authenticated epoch");
        let replay = rpc::gen_tonic_replay_scope_headers(
            LEGACY_RENAME_AUDIENCE,
            LEGACY_RENAME_RPC_PATH,
            headers["x-rustfs-timestamp"].to_str().expect("signed timestamp"),
            headers["x-rustfs-content-sha256"].to_str().expect("signed digest"),
            epoch,
        )
        .expect("real replay-scoped signature");
        headers.extend(replay);
        request.metadata_mut().as_mut().extend(headers);
        request
    }

    async fn authenticated_legacy_rename(
        service: &NodeService,
        request: Request<RenameDataRequest>,
    ) -> Result<Response<RenameDataResponse>, Status> {
        // This is the production HMAC/replay verifier used by check_auth. The
        // handler then independently verifies the signed canonical body digest.
        crate::storage::storage_api::ecstore_rpc::verify_tonic_rpc_signature_with_bootstrap(
            LEGACY_RENAME_AUDIENCE,
            LEGACY_RENAME_RPC_PATH,
            request.metadata().as_ref(),
            false,
        )
        .map_err(|_| Status::unauthenticated("legacy rename signature rejected"))?;
        service.handle_rename_data(request).await
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
