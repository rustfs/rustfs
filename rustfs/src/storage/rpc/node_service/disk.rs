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

use super::NodeService;
use crate::storage::storage_api::rpc_consumer::node_service::{
    BatchReadVersionReq, BatchReadVersionResp, DeleteOptions, DiskError, DiskInfoOptions, FileInfoVersions, ReadMultipleReq,
    ReadMultipleResp, ReadOptions, StorageDiskRpcExt as _, UpdateMetadataOpts, validate_batch_read_version_item_count,
};
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use crate::storage::storage_api::{PartTransactionAction, RenameDataResp, SnapshotLeaseToken, verify_tonic_mutation_body_digest};
use bytes::Bytes;
use rustfs_filemeta::FileInfo;
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_MSGPACK_CODEC_JSON, INTERNODE_MSGPACK_CODEC_MSGPACK, INTERNODE_MSGPACK_DIRECTION_REQUEST,
    INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION, INTERNODE_OPERATION_GRPC_READ_ALL, INTERNODE_OPERATION_GRPC_READ_VERSION,
    INTERNODE_OPERATION_GRPC_WRITE_ALL, INTERNODE_STAGE_BATCH_READ_VERSION_DISK_READ,
    INTERNODE_STAGE_BATCH_READ_VERSION_REQUEST_DECODE, INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_JSON_ENCODE,
    INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_MSGPACK_ENCODE, INTERNODE_STAGE_READ_VERSION_DISK_READ,
    INTERNODE_STAGE_READ_VERSION_REQUEST_DECODE, INTERNODE_STAGE_READ_VERSION_RESPONSE_JSON_ENCODE,
    INTERNODE_STAGE_READ_VERSION_RESPONSE_MSGPACK_ENCODE, INTERNODE_TRANSPORT_BACKEND_GRPC, global_internode_metrics,
};
use rustfs_protos::proto_gen::node_service::*;
use serde::de::DeserializeOwned;
use std::io::Cursor;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::debug;

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

struct DecodedRpcPayload<T> {
    value: T,
    from_msgpack: bool,
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

fn decode_rename_data_request_file_info(
    binary: &[u8],
    json: &str,
) -> std::result::Result<DecodedRpcPayload<FileInfo>, DiskError> {
    decode_msgpack_or_json_with_source(binary, json, "FileInfo")
}

fn encode_rename_data_response_payloads(
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
        if let Some(disk) = self.find_disk(&request.disk).await {
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
        if let Some(disk) = self.find_disk(&request.disk).await {
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
    ) -> Result<Response<DeleteVersionResponse>, Status> {
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_delete_version_request_body(request.get_ref()),
            "delete_version",
        )?;
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
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
            match disk
                .delete_version(&request.volume, &request.path, file_info, request.force_del_marker, opts)
                .await
            {
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
        } else {
            Ok(Response::new(DeleteVersionResponse {
                success: false,
                raw_file_info: "".to_string(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
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
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match decode_msgpack_or_json::<FileInfo>(&request.file_info_bin, &request.file_info, "FileInfo") {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(WriteMetadataResponse {
                        success: false,
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            match disk.write_metadata("", &request.volume, &request.path, file_info).await {
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
        if let Some(disk) = self.find_disk(&request.disk).await {
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
        if let Some(disk) = self.find_disk(&request.disk).await {
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
        verify_disk_mutation_digest(
            &request,
            rustfs_protos::canonical_rename_data_request_body(request.get_ref()),
            "rename_data",
        )?;
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
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
            let request_decoded_from_msgpack = decoded_file_info.from_msgpack;
            match disk
                .rename_data(
                    &request.src_volume,
                    &request.src_path,
                    &decoded_file_info.value,
                    &request.dst_volume,
                    &request.dst_path,
                )
                .await
            {
                Ok(rename_data_resp) => {
                    match encode_rename_data_response_payloads(&rename_data_resp, request_decoded_from_msgpack) {
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
                    }
                }
                Err(err) => Ok(Response::new(RenameDataResponse {
                    success: false,
                    rename_data_resp: String::new(),
                    rename_data_resp_bin: Vec::new().into(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(RenameDataResponse {
                success: false,
                rename_data_resp: String::new(),
                rename_data_resp_bin: Vec::new().into(),
                error: Some(DiskError::other("cannot find disk".to_string()).into()),
            }))
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
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk
                .rename_file(&request.src_volume, &request.src_path, &request.dst_volume, &request.dst_path)
                .await
            {
                Ok(_) => Ok(Response::new(RenameFileResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(RenameFileResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(RenameFileResponse {
                success: false,
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
        if let Some(disk) = self.find_disk(&request.disk).await {
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
        if let Some(disk) = self.find_disk(&request.disk).await {
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
        if let Some(disk) = self.find_disk(&request.disk).await {
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
        verify_disk_mutation_digest(&request, rustfs_protos::canonical_delete_request_body(request.get_ref()), "delete")?;
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let options = match serde_json::from_str::<DeleteOptions>(&request.options) {
                Ok(options) => options,
                Err(err) => {
                    return Ok(Response::new(DeleteResponse {
                        success: false,
                        error: Some(DiskError::other(format!("decode DeleteOptions failed: {err}")).into()),
                    }));
                }
            };
            match disk.delete(&request.volume, &request.path, options).await {
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
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.write_all(&request.volume, &request.path, request.data).await {
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
