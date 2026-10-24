// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::{LocalMutationTarget, NodeService, ResponseStream};
use crate::storage::storage_api::ecstore_disk::{ArtifactInventoryFrame as LocalFrame, ArtifactInventoryVersion as LocalVersion};
use crate::storage::storage_api::{sign_tonic_rpc_response_proof, verify_tonic_canonical_body_digest};
use prost::Message;
use rustfs_protos::{
    artifact_inventory as protocol,
    proto_gen::node_service::{
        ArtifactInventoryComplete, ArtifactInventoryEntries, ArtifactInventoryFrame, ArtifactInventoryPhase,
        ArtifactInventoryRequest, ArtifactInventoryVerified, ArtifactInventoryVersion, artifact_inventory_frame::Payload,
    },
};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

fn signed(request: &ArtifactInventoryRequest, frame: &mut ArtifactInventoryFrame) -> Result<(), Status> {
    let canonical =
        protocol::canonical_frame(request, frame).map_err(|_| Status::internal("inventory frame authentication overflow"))?;
    frame.response_proof = sign_tonic_rpc_response_proof(&canonical)
        .map_err(|_| Status::unavailable("inventory response authentication unavailable"))?
        .into();
    #[cfg(test)]
    test_fault::alter(request, frame)?;
    if frame.encoded_len() > protocol::MAX_FRAME_BYTES {
        return Err(Status::resource_exhausted("inventory encoded frame exceeds limit"));
    }
    Ok(())
}

impl NodeService {
    pub(super) async fn handle_inventory_artifact_attempt(
        &self,
        request: Request<ArtifactInventoryRequest>,
    ) -> Result<Response<ResponseStream<ArtifactInventoryFrame>>, Status> {
        let canonical = protocol::canonical_request(request.get_ref())
            .map_err(|_| Status::invalid_argument("inventory request authentication overflow"))?;
        verify_tonic_canonical_body_digest(&request, &canonical)
            .map_err(|_| Status::permission_denied("inventory requires body-bound v2 authentication"))?;
        protocol::validate_request(request.get_ref()).map_err(|_| Status::invalid_argument("invalid inventory request"))?;
        let LocalMutationTarget::Ready(store) = self.local_mutation_target() else {
            return Err(Status::unavailable("inventory requires ready storage"));
        };
        let body = request.into_inner();
        let expected = Uuid::from_slice(&body.expected_disk_id).map_err(|_| Status::invalid_argument("disk UUID"))?;
        let incarnation =
            Uuid::from_slice(&body.bucket_incarnation_id).map_err(|_| Status::invalid_argument("incarnation UUID"))?;
        let phase = ArtifactInventoryPhase::try_from(body.phase).map_err(|_| Status::invalid_argument("inventory phase"))?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(body.timeout_millis);
        let (sender, receiver) = mpsc::channel(2);
        tokio::spawn(async move {
            let run = async {
                let (output, mut frames) = mpsc::channel(1);
                let operation = store.inventory_local_artifact_attempt(
                    (&body.disk, expected),
                    (&body.bucket, incarnation),
                    &body.attempt_prefix,
                    phase,
                    deadline,
                    output,
                );
                tokio::pin!(operation);
                let mut terminal = None;
                let mut sequence = 0;
                let mut ended = false;
                loop {
                    tokio::select! {
                        biased;
                        _ = sender.closed() => return Ok::<_, Status>(()),
                        result = &mut operation => {
                            result.map_err(|error| Status::failed_precondition(format!("inventory scan failed: {error}")))?;
                            // The scanner's bounded channel may still contain its
                            // last frame when its future completes successfully.
                            while let Some(frame) = frames.recv().await {
                                match frame {
                                    LocalFrame::Entries { versions, .. } => {
                                        let mut frame = entry_frame(sequence, versions);
                                        signed(&body, &mut frame)?;
                                        sender.send(Ok(frame)).await.map_err(|_| Status::cancelled("inventory consumer closed"))?;
                                        sequence += 1;
                                    }
                                    LocalFrame::Complete { entries, .. } => terminal = Some(entries),
                                }
                            }
                            break;
                        }
                        frame = frames.recv(), if !ended => match frame {
                            Some(LocalFrame::Entries { versions, .. }) => {
                                let mut frame = entry_frame(sequence, versions);
                                signed(&body, &mut frame)?;
                                sender.send(Ok(frame)).await.map_err(|_| Status::cancelled("inventory consumer closed"))?;
                                sequence += 1;
                            }
                            Some(LocalFrame::Complete { entries, .. }) => terminal = Some(entries),
                            None => ended = true,
                        },
                    }
                }
                let payload = match phase {
                    ArtifactInventoryPhase::ArtifactInventoryScan => Payload::Complete(ArtifactInventoryComplete {
                        disk_id: body.expected_disk_id.clone(),
                        entries: terminal.ok_or_else(|| Status::data_loss("inventory terminal missing"))?,
                    }),
                    ArtifactInventoryPhase::ArtifactInventoryVerify => Payload::Verified(ArtifactInventoryVerified {
                        disk_id: body.expected_disk_id.clone(),
                    }),
                };
                let mut frame = ArtifactInventoryFrame {
                    sequence,
                    payload: Some(payload),
                    response_proof: Default::default(),
                };
                signed(&body, &mut frame)?;
                #[cfg(test)]
                if test_fault::mode(&body) == 4 {
                    return Ok(());
                }
                #[cfg(test)]
                let extra = frame.clone();
                sender
                    .send(Ok(frame))
                    .await
                    .map_err(|_| Status::cancelled("inventory consumer closed"))?;
                #[cfg(test)]
                match test_fault::mode(&body) {
                    5 => {
                        sender
                            .send(Ok(extra))
                            .await
                            .map_err(|_| Status::cancelled("inventory consumer closed"))?;
                    }
                    6 => {
                        sender
                            .send(Err(Status::data_loss("injected trailer failure after Complete")))
                            .await
                            .map_err(|_| Status::cancelled("inventory consumer closed"))?;
                    }
                    _ => {}
                }
                Ok(())
            };
            let result = tokio::time::timeout_at(deadline, run)
                .await
                .unwrap_or_else(|_| Err(Status::deadline_exceeded("inventory deadline exceeded")));
            if let Err(error) = result {
                let _ = sender.try_send(Err(error));
            }
        });
        Ok(Response::new(Box::pin(tokio_stream::wrappers::ReceiverStream::new(receiver))))
    }
}

fn entry_frame(sequence: u64, versions: Vec<LocalVersion>) -> ArtifactInventoryFrame {
    ArtifactInventoryFrame {
        sequence,
        response_proof: Default::default(),
        payload: Some(Payload::Entries(ArtifactInventoryEntries {
            versions: versions
                .into_iter()
                .map(|version| ArtifactInventoryVersion {
                    key: version.key,
                    version_id: version.version_id.as_bytes().to_vec().into(),
                    delete_marker: version.delete_marker,
                })
                .collect(),
        })),
    }
}

#[cfg(test)]
mod test_fault {
    use super::{
        ArtifactInventoryFrame, ArtifactInventoryRequest, ArtifactInventoryVerified, Payload, protocol,
        sign_tonic_rpc_response_proof,
    };
    use tonic::Status;
    use uuid::Uuid;
    static FAULT: std::sync::Mutex<Option<(Uuid, u8)>> = std::sync::Mutex::new(None);
    pub(super) fn set(disk: Uuid, mode: u8) {
        *FAULT.lock().expect("inventory fault") = Some((disk, mode));
    }
    pub(super) fn mode(request: &ArtifactInventoryRequest) -> u8 {
        FAULT
            .lock()
            .expect("inventory fault")
            .as_ref()
            .filter(|(disk, _)| disk.as_bytes() == request.expected_disk_id.as_ref())
            .map_or(0, |(_, mode)| *mode)
    }
    pub(super) fn alter(request: &ArtifactInventoryRequest, frame: &mut ArtifactInventoryFrame) -> Result<(), Status> {
        let mut scope = request.clone();
        match mode(request) {
            1 => {
                frame.response_proof.clear();
                return Ok(());
            }
            2 => scope.invocation_id = Uuid::new_v4().as_bytes().to_vec().into(),
            3 => frame.sequence += 1,
            7 => {
                if let Some(Payload::Complete(complete)) = &mut frame.payload {
                    complete.entries += 1;
                }
            }
            8 => {
                if matches!(frame.payload, Some(Payload::Complete(_))) {
                    frame.payload = Some(Payload::Verified(ArtifactInventoryVerified {
                        disk_id: request.expected_disk_id.clone(),
                    }));
                }
            }
            _ => return Ok(()),
        }
        let canonical = protocol::canonical_frame(&scope, frame).map_err(|_| Status::internal("fault canonical"))?;
        frame.response_proof = sign_tonic_rpc_response_proof(&canonical)
            .map_err(|_| Status::unavailable("fault proof"))?
            .into();
        Ok(())
    }
}

#[cfg(test)]
#[path = "inventory_tests.rs"]
mod tests;
