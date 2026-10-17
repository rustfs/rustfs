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

use super::disk::{decode_rename_data_request_file_info, encode_rename_data_response_payloads};
use super::{LocalMutationTarget, NodeService};
use crate::storage::storage_api::rpc_consumer::node_service::DiskError;
use crate::storage::storage_api::{sign_tonic_rpc_response_proof, verify_tonic_canonical_body_digest};
use bytes::Bytes;
use rustfs_protos::proto_gen::node_service::{
    RenameDataRequest, RenameDataResponse, SealArtifactAttemptPutsRequest, SealArtifactAttemptPutsResponse,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

fn required_uuid(value: &[u8], field: &'static str) -> Result<Uuid, Status> {
    Uuid::from_slice(value)
        .ok()
        .filter(|id| !id.is_nil())
        .ok_or_else(|| Status::invalid_argument(format!("{field} must be a non-nil UUID")))
}

fn rename_failure(error: DiskError) -> Response<RenameDataResponse> {
    Response::new(RenameDataResponse {
        success: false,
        rename_data_resp: String::new(),
        error: Some(error.into()),
        rename_data_resp_bin: Bytes::new(),
    })
}

impl NodeService {
    pub(super) async fn handle_artifact_rename_data(
        &self,
        request: Request<RenameDataRequest>,
    ) -> Result<Response<RenameDataResponse>, Status> {
        let canonical = rustfs_protos::canonical_artifact_rename_data_request_body(request.get_ref())
            .map_err(|_| Status::invalid_argument("artifact rename request is too large to authenticate"))?;
        verify_tonic_canonical_body_digest(&request, &canonical)
            .map_err(|_| Status::permission_denied("artifact rename requires its body-bound authentication"))?;
        let incarnation = required_uuid(&request.get_ref().bucket_incarnation_id, "bucket incarnation")?;
        if !request.get_ref().scanner_publication_lease_token.is_empty() {
            return Err(Status::invalid_argument("artifact rename cannot carry a scanner publication lease"));
        }
        // Capture once: a bootstrap/unbound request cannot switch to a ready
        // instance after any await, and no global store fallback is permitted.
        let LocalMutationTarget::Ready(store) = self.local_mutation_target() else {
            return Err(Status::unavailable("artifact rename requires a ready storage instance"));
        };
        let request = request.into_inner();
        let decoded = match decode_rename_data_request_file_info(&request.file_info_bin, &request.file_info) {
            Ok(decoded) => decoded,
            Err(error) => return Ok(rename_failure(error)),
        };
        let result = store
            .rename_local_artifact_data(
                &request.disk,
                (&request.src_volume, &request.src_path),
                &decoded.value,
                (&request.dst_volume, &request.dst_path),
                incarnation,
            )
            .await
            .and_then(|response| encode_rename_data_response_payloads(&response, decoded.from_msgpack));
        Ok(match result {
            Ok((rename_data_resp, rename_data_resp_bin)) => Response::new(RenameDataResponse {
                success: true,
                rename_data_resp,
                rename_data_resp_bin: rename_data_resp_bin.into(),
                error: None,
            }),
            Err(error) => rename_failure(error),
        })
    }

    pub(super) async fn handle_seal_artifact_attempt_puts(
        &self,
        request: Request<SealArtifactAttemptPutsRequest>,
    ) -> Result<Response<SealArtifactAttemptPutsResponse>, Status> {
        let canonical = rustfs_protos::canonical_seal_artifact_attempt_puts_request_body(request.get_ref())
            .map_err(|_| Status::invalid_argument("artifact PUT seal request is too large to authenticate"))?;
        verify_tonic_canonical_body_digest(&request, &canonical)
            .map_err(|_| Status::permission_denied("artifact PUT seal requires its body-bound authentication"))?;
        let incarnation = required_uuid(&request.get_ref().bucket_incarnation_id, "bucket incarnation")?;
        let expected_disk = required_uuid(&request.get_ref().expected_disk_id, "expected disk identity")?;
        let LocalMutationTarget::Ready(store) = self.local_mutation_target() else {
            return Err(Status::unavailable("artifact PUT seal requires a ready storage instance"));
        };
        let request = request.into_inner();
        let result = store
            .seal_local_artifact_puts(&request.disk, expected_disk, &request.bucket, incarnation, &request.attempt_prefix)
            .await;
        // Success is available only after the exact receiving disk has both
        // persisted closure and drained its physical publication owners.
        let mut response = match result {
            Ok(disk_id) => SealArtifactAttemptPutsResponse {
                success: true,
                disk_id: disk_id.as_bytes().to_vec().into(),
                error: None,
                response_proof: Bytes::new(),
            },
            Err(error) => SealArtifactAttemptPutsResponse {
                success: false,
                error: Some(error.into()),
                disk_id: Bytes::new(),
                response_proof: Bytes::new(),
            },
        };
        let canonical = rustfs_protos::canonical_seal_artifact_attempt_puts_response_body(&request, &response)
            .map_err(|_| Status::internal("artifact PUT seal response is too large to authenticate"))?;
        response.response_proof = sign_tonic_rpc_response_proof(&canonical)
            .map_err(|_| Status::unavailable("artifact PUT seal response authentication is unavailable"))?
            .into();
        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::rpc::node_service::make_server_for_context;
    use crate::storage::storage_api::set_tonic_canonical_body_digest;

    fn authenticated<T>(message: T, canonical: &[u8]) -> Request<T> {
        let mut request = Request::new(message);
        request
            .metadata_mut()
            .insert("x-rustfs-rpc-auth-version", "2".parse().expect("v2 auth metadata"));
        set_tonic_canonical_body_digest(&mut request, canonical).expect("canonical digest metadata");
        request
    }

    fn rename() -> RenameDataRequest {
        RenameDataRequest {
            disk: "disk-ref".into(),
            src_volume: ".rustfs.sys/tmp".into(),
            src_path: "staged".into(),
            dst_volume: "artifacts".into(),
            dst_path: "outputs/tenant/run/attempt/image".into(),
            bucket_incarnation_id: vec![1; 16].into(),
            ..Default::default()
        }
    }

    fn seal() -> SealArtifactAttemptPutsRequest {
        SealArtifactAttemptPutsRequest {
            disk: "disk-ref".into(),
            bucket: "artifacts".into(),
            bucket_incarnation_id: vec![1; 16].into(),
            attempt_prefix: "outputs/tenant/run/attempt".into(),
            expected_disk_id: vec![2; 16].into(),
        }
    }

    #[tokio::test]
    async fn artifact_rpcs_require_v2_body_digest_before_target_lookup() {
        let service = make_server_for_context(None);
        for digest in [None, Some("UNSIGNED-PAYLOAD"), Some("invalid")] {
            let mut rename = Request::new(rename());
            let mut seal = Request::new(seal());
            for metadata in [rename.metadata_mut(), seal.metadata_mut()] {
                metadata.insert("x-rustfs-rpc-auth-version", "2".parse().expect("v2 metadata"));
                if let Some(digest) = digest {
                    metadata.insert("x-rustfs-content-sha256", digest.parse().expect("digest metadata"));
                }
            }
            assert_eq!(
                service
                    .handle_artifact_rename_data(rename)
                    .await
                    .expect_err("mandatory rename digest")
                    .code(),
                tonic::Code::PermissionDenied
            );
            assert_eq!(
                service
                    .handle_seal_artifact_attempt_puts(seal)
                    .await
                    .expect_err("mandatory seal digest")
                    .code(),
                tonic::Code::PermissionDenied
            );
        }
        let message = rename();
        let body = rustfs_protos::canonical_artifact_rename_data_request_body(&message).expect("rename canonical");
        let mut request = authenticated(message, &body);
        request.metadata_mut().remove("x-rustfs-rpc-auth-version");
        assert_eq!(
            service
                .handle_artifact_rename_data(request)
                .await
                .expect_err("digest alone is not v2 authentication")
                .code(),
            tonic::Code::PermissionDenied
        );
        let message = seal();
        let body = rustfs_protos::canonical_seal_artifact_attempt_puts_request_body(&message).expect("seal canonical");
        let mut request = authenticated(message, &body);
        request.metadata_mut().remove("x-rustfs-rpc-auth-version");
        assert_eq!(
            service
                .handle_seal_artifact_attempt_puts(request)
                .await
                .expect_err("digest alone is not v2 authentication")
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[tokio::test]
    async fn artifact_rename_rejects_legacy_domain_scanner_and_malformed_incarnation() {
        let service = make_server_for_context(None);
        let message = rename();
        let legacy = rustfs_protos::canonical_rename_data_request_body(&message).expect("legacy canonical");
        assert_eq!(
            service
                .handle_artifact_rename_data(authenticated(message, &legacy))
                .await
                .expect_err("legacy digest cannot authenticate artifact method")
                .code(),
            tonic::Code::PermissionDenied
        );
        for invalid in [Vec::new(), vec![0; 16], vec![1; 15]] {
            let message = RenameDataRequest {
                bucket_incarnation_id: invalid.into(),
                ..rename()
            };
            let body =
                rustfs_protos::canonical_artifact_rename_data_request_body(&message).expect("malformed identity canonical");
            assert_eq!(
                service
                    .handle_artifact_rename_data(authenticated(message, &body))
                    .await
                    .expect_err("malformed incarnation is rejected before lookup")
                    .code(),
                tonic::Code::InvalidArgument
            );
        }
        let message = RenameDataRequest {
            scanner_publication_lease_token: vec![1; 16].into(),
            ..rename()
        };
        let body = rustfs_protos::canonical_artifact_rename_data_request_body(&message).expect("scanner canonical");
        assert_eq!(
            service
                .handle_artifact_rename_data(authenticated(message, &body))
                .await
                .expect_err("scanner lease cannot be mixed into artifact publication")
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn artifact_seal_rejects_malformed_incarnation_and_disk_identity() {
        let service = make_server_for_context(None);
        for invalid in [Vec::new(), vec![0; 16], vec![1; 15]] {
            for field in ["incarnation", "disk"] {
                let mut message = seal();
                if field == "incarnation" {
                    message.bucket_incarnation_id = invalid.clone().into();
                } else {
                    message.expected_disk_id = invalid.clone().into();
                }
                let body =
                    rustfs_protos::canonical_seal_artifact_attempt_puts_request_body(&message).expect("malformed seal canonical");
                assert_eq!(
                    service
                        .handle_seal_artifact_attempt_puts(authenticated(message, &body))
                        .await
                        .expect_err("seal identities must be non-nil UUIDs before lookup")
                        .code(),
                    tonic::Code::InvalidArgument
                );
            }
        }
    }

    #[tokio::test]
    async fn artifact_rpcs_reject_body_tampering_before_target_lookup() {
        let service = make_server_for_context(None);
        let original = rename();
        let body = rustfs_protos::canonical_artifact_rename_data_request_body(&original).expect("rename canonical");
        for mutate in [
            |request: &mut RenameDataRequest| request.disk = "other-disk".into(),
            |request: &mut RenameDataRequest| request.bucket_incarnation_id = vec![3; 16].into(),
            |request: &mut RenameDataRequest| request.dst_path = "other-attempt/image".into(),
            |request: &mut RenameDataRequest| request.file_info_bin = vec![0x80].into(),
        ] {
            let mut message = original.clone();
            mutate(&mut message);
            assert_eq!(
                service
                    .handle_artifact_rename_data(authenticated(message, &body))
                    .await
                    .expect_err("artifact rename body is authenticated")
                    .code(),
                tonic::Code::PermissionDenied
            );
        }
        let original = seal();
        let body = rustfs_protos::canonical_seal_artifact_attempt_puts_request_body(&original).expect("seal canonical");
        for mutate in [
            |request: &mut SealArtifactAttemptPutsRequest| request.disk = "other-disk".into(),
            |request: &mut SealArtifactAttemptPutsRequest| request.bucket = "other-bucket".into(),
            |request: &mut SealArtifactAttemptPutsRequest| request.bucket_incarnation_id = vec![3; 16].into(),
            |request: &mut SealArtifactAttemptPutsRequest| request.attempt_prefix = "other-attempt".into(),
            |request: &mut SealArtifactAttemptPutsRequest| request.expected_disk_id = vec![4; 16].into(),
        ] {
            let mut message = original.clone();
            mutate(&mut message);
            assert_eq!(
                service
                    .handle_seal_artifact_attempt_puts(authenticated(message, &body))
                    .await
                    .expect_err("artifact seal body is authenticated")
                    .code(),
                tonic::Code::PermissionDenied
            );
        }
    }

    #[tokio::test]
    async fn artifact_rpcs_reject_unbound_target_without_global_fallback() {
        let service = make_server_for_context(None);
        let message = rename();
        let body = rustfs_protos::canonical_artifact_rename_data_request_body(&message).expect("rename canonical");
        assert_eq!(
            service
                .handle_artifact_rename_data(authenticated(message, &body))
                .await
                .expect_err("rename needs a captured ready instance")
                .code(),
            tonic::Code::Unavailable
        );
        let message = seal();
        let body = rustfs_protos::canonical_seal_artifact_attempt_puts_request_body(&message).expect("seal canonical");
        assert_eq!(
            service
                .handle_seal_artifact_attempt_puts(authenticated(message, &body))
                .await
                .expect_err("seal needs a captured ready instance")
                .code(),
            tonic::Code::Unavailable
        );
    }
}
