// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use crate::{
    CanonicalBodyBuilder,
    proto_gen::node_service::{ArtifactInventoryFrame, ArtifactInventoryRequest},
};
use prost::Message;
use std::io;
use uuid::Uuid;

pub const VERSION: u32 = 1;
pub const MAX_FRAME_BYTES: usize = 64 * 1024;
pub const MAX_FRAME_ENTRIES: usize = 256;
pub const MAX_TIMEOUT_MILLIS: u64 = 300_000;

pub fn canonical_request(request: &ArtifactInventoryRequest) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-artifact-inventory-request-v1\0");
    body.push_u32(request.protocol_version);
    body.push_bytes(&request.invocation_id)?;
    body.push_str(&request.disk)?;
    body.push_bytes(&request.expected_disk_id)?;
    body.push_str(&request.bucket)?;
    body.push_bytes(&request.bucket_incarnation_id)?;
    body.push_str(&request.attempt_prefix)?;
    body.push_u64(request.timeout_millis);
    body.push_u32(u32::try_from(request.phase)?);
    Ok(body.finish())
}

pub fn canonical_frame(
    request: &ArtifactInventoryRequest,
    frame: &ArtifactInventoryFrame,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-artifact-inventory-frame-v1\0");
    body.push_bytes(&canonical_request(request)?)?;
    let mut unsigned = frame.clone();
    unsigned.response_proof.clear();
    body.push_bytes(&unsigned.encode_to_vec())?;
    Ok(body.finish())
}

impl crate::CanonicalMutationBody for ArtifactInventoryRequest {
    fn canonical_body(&self) -> Result<Vec<u8>, std::num::TryFromIntError> {
        canonical_request(self)
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn non_nil(bytes: &[u8]) -> bool {
    Uuid::from_slice(bytes).is_ok_and(|id| !id.is_nil())
}

pub fn validate_request(request: &ArtifactInventoryRequest) -> io::Result<()> {
    if request.protocol_version != VERSION
        || !non_nil(&request.invocation_id)
        || !non_nil(&request.expected_disk_id)
        || !non_nil(&request.bucket_incarnation_id)
        || request.disk.is_empty()
        || request.disk.len() > 4096
        || request.bucket.is_empty()
        || request.bucket.len() > 255
        || request.attempt_prefix.len() >= 1024
        || !request.attempt_prefix.ends_with('/')
        || request.timeout_millis == 0
        || request.timeout_millis > MAX_TIMEOUT_MILLIS
        || !matches!(request.phase, 0 | 1)
    {
        return Err(invalid("invalid artifact inventory request"));
    }
    Ok(())
}

/// One response stream, finalized only after authenticated terminal and clean EOF.
pub struct StreamState {
    next: u64,
    entries: u64,
    terminal: bool,
}
impl Default for StreamState {
    fn default() -> Self {
        Self::new()
    }
}
impl StreamState {
    pub fn new() -> Self {
        Self {
            next: 0,
            entries: 0,
            terminal: false,
        }
    }
    pub fn accept(
        &mut self,
        request: &ArtifactInventoryRequest,
        frame: &ArtifactInventoryFrame,
        verify: impl FnOnce(&[u8], &[u8]) -> bool,
    ) -> io::Result<()> {
        use crate::proto_gen::node_service::artifact_inventory_frame::Payload;
        validate_request(request)?;
        if self.terminal || frame.sequence != self.next || frame.encoded_len() > MAX_FRAME_BYTES {
            return Err(invalid("artifact inventory frame sequence or size mismatch"));
        }
        let canonical = canonical_frame(request, frame).map_err(io::Error::other)?;
        if !verify(&canonical, &frame.response_proof) {
            return Err(invalid("artifact inventory frame proof mismatch"));
        }
        match &frame.payload {
            Some(Payload::Entries(entries)) if request.phase == 0 => {
                if entries.versions.is_empty() || entries.versions.len() > MAX_FRAME_ENTRIES {
                    return Err(invalid("artifact inventory entry count is invalid"));
                }
                for version in &entries.versions {
                    if version.key.len() > 1024
                        || version.key.len() <= request.attempt_prefix.len()
                        || !version.key.starts_with(&request.attempt_prefix)
                        || !non_nil(&version.version_id)
                    {
                        return Err(invalid("artifact inventory version scope is invalid"));
                    }
                }
                self.entries = self
                    .entries
                    .checked_add(u64::try_from(entries.versions.len()).map_err(io::Error::other)?)
                    .ok_or_else(|| invalid("artifact inventory count overflow"))?;
            }
            Some(Payload::Complete(complete))
                if request.phase == 0 && complete.disk_id == request.expected_disk_id && complete.entries == self.entries =>
            {
                self.terminal = true
            }
            Some(Payload::Verified(verified)) if request.phase == 1 && verified.disk_id == request.expected_disk_id => {
                self.terminal = true
            }
            _ => return Err(invalid("artifact inventory terminal, phase or identity mismatch")),
        }
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| invalid("artifact inventory sequence overflow"))?;
        Ok(())
    }
    pub fn finish(&self) -> io::Result<()> {
        if self.terminal {
            Ok(())
        } else {
            Err(invalid("artifact inventory stream is incomplete"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto_gen::node_service::{
        ArtifactInventoryComplete, ArtifactInventoryVerified, artifact_inventory_frame::Payload,
    };
    fn request() -> ArtifactInventoryRequest {
        ArtifactInventoryRequest {
            protocol_version: VERSION,
            invocation_id: Uuid::new_v4().as_bytes().to_vec().into(),
            disk: "disk".into(),
            expected_disk_id: Uuid::new_v4().as_bytes().to_vec().into(),
            bucket: "artifacts".into(),
            bucket_incarnation_id: Uuid::new_v4().as_bytes().to_vec().into(),
            attempt_prefix: "outputs/tenant/run/attempt/".into(),
            timeout_millis: 30_000,
            phase: 0,
        }
    }
    #[test]
    fn artifact_inventory_stream_rejects_scope_sequence_and_truncation() {
        let request = request();
        let frame = ArtifactInventoryFrame {
            sequence: 0,
            payload: Some(Payload::Complete(ArtifactInventoryComplete {
                disk_id: request.expected_disk_id.clone(),
                entries: 0,
            })),
            response_proof: vec![7; 32].into(),
        };
        let mut state = StreamState::new();
        assert!(state.finish().is_err(), "EOF alone is incomplete");
        state.accept(&request, &frame, |_, _| true).expect("A03I2_SIGNED_COMPLETION");
        state.finish().expect("authenticated terminal and EOF");
        assert!(state.accept(&request, &frame, |_, _| true).is_err(), "no data after Complete");
        let mut wrong = frame.clone();
        wrong.sequence = 1;
        assert!(
            StreamState::new().accept(&request, &wrong, |_, _| true).is_err(),
            "sequence starts at zero"
        );
        wrong = frame.clone();
        wrong.payload = Some(Payload::Verified(ArtifactInventoryVerified {
            disk_id: request.expected_disk_id.clone(),
        }));
        assert!(
            StreamState::new().accept(&request, &wrong, |_, _| true).is_err(),
            "verify is not scan completion"
        );
        let canonical = canonical_frame(&request, &frame).expect("canonical baseline");
        let mut changed = request.clone();
        changed.invocation_id = Uuid::new_v4().as_bytes().to_vec().into();
        assert_ne!(canonical, canonical_frame(&changed, &frame).expect("invocation"));
        changed = request.clone();
        changed.phase = 1;
        assert_ne!(canonical, canonical_frame(&changed, &frame).expect("phase"));
    }
    #[test]
    fn artifact_inventory_encoded_limits_and_full_request_binding() {
        use crate::proto_gen::node_service::{ArtifactInventoryEntries, ArtifactInventoryVersion};
        let baseline = request();
        let body = canonical_request(&baseline).expect("canonical request");
        let changes: Vec<fn(&mut ArtifactInventoryRequest)> = vec![
            |r| r.protocol_version += 1,
            |r| r.invocation_id = Uuid::new_v4().as_bytes().to_vec().into(),
            |r| r.disk.push('x'),
            |r| r.expected_disk_id = Uuid::new_v4().as_bytes().to_vec().into(),
            |r| r.bucket.push('x'),
            |r| r.bucket_incarnation_id = Uuid::new_v4().as_bytes().to_vec().into(),
            |r| r.attempt_prefix.push_str("other/"),
            |r| r.timeout_millis += 1,
            |r| r.phase = 1,
        ];
        for change in changes {
            let mut changed = baseline.clone();
            change(&mut changed);
            assert_ne!(body, canonical_request(&changed).expect("bound field"));
        }
        let entry = ArtifactInventoryVersion {
            key: format!("{}object", baseline.attempt_prefix),
            version_id: Uuid::new_v4().as_bytes().to_vec().into(),
            delete_marker: false,
        };
        let frame = ArtifactInventoryFrame {
            sequence: 0,
            payload: Some(Payload::Entries(ArtifactInventoryEntries { versions: vec![entry] })),
            response_proof: vec![1; 32].into(),
        };
        let canonical = canonical_frame(&baseline, &frame).expect("baseline frame");
        for change in [0, 1, 2] {
            let mut changed = frame.clone();
            if let Some(Payload::Entries(entries)) = changed.payload.as_mut() {
                match change {
                    0 => entries.versions[0].key.push('x'),
                    1 => entries.versions[0].version_id = Uuid::new_v4().as_bytes().to_vec().into(),
                    _ => entries.versions[0].delete_marker = true,
                }
            }
            assert_ne!(canonical, canonical_frame(&baseline, &changed).expect("all version fields bound"));
        }
        assert!(StreamState::new().accept(&baseline, &frame, |_, _| false).is_err(), "proof is mandatory");
        let mut oversized = frame.clone();
        if let Some(Payload::Entries(entries)) = oversized.payload.as_mut() {
            entries.versions[0].key = "x".repeat(MAX_FRAME_BYTES);
        }
        assert!(
            StreamState::new()
                .accept(&baseline, &oversized, |_, _| panic!("encoded limit checked before canonical copy"))
                .is_err()
        );
        let mut wrong = frame.clone();
        if let Some(Payload::Entries(entries)) = wrong.payload.as_mut() {
            entries.versions[0].version_id = vec![0; 16].into();
        }
        assert!(StreamState::new().accept(&baseline, &wrong, |_, _| true).is_err());
        let mut state = StreamState::new();
        state.accept(&baseline, &frame, |_, _| true).expect("one actual observation");
        assert!(state.finish().is_err(), "partial entries plus clean EOF is incomplete");
        let mut complete = ArtifactInventoryFrame {
            sequence: 1,
            payload: Some(Payload::Complete(ArtifactInventoryComplete {
                disk_id: baseline.expected_disk_id.clone(),
                entries: 0,
            })),
            response_proof: vec![1; 32].into(),
        };
        assert!(state.accept(&baseline, &complete, |_, _| true).is_err(), "terminal total must match");
        if let Some(Payload::Complete(value)) = complete.payload.as_mut() {
            value.entries = 1;
        }
        state.accept(&baseline, &complete, |_, _| true).expect("exact total");
        state.finish().expect("complete");
        let mut verify = baseline;
        verify.phase = 1;
        let terminal = ArtifactInventoryFrame {
            sequence: 0,
            payload: Some(Payload::Verified(ArtifactInventoryVerified {
                disk_id: verify.expected_disk_id.clone(),
            })),
            response_proof: vec![1; 32].into(),
        };
        StreamState::new()
            .accept(&verify, &terminal, |_, _| true)
            .expect("distinct read-only identity proof");
    }
}
