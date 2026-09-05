// Copyright 2024 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Bounded, authenticated receiver contract for per-bucket dirty acknowledgements.

use crate::CanonicalBodyBuilder;
use crate::proto_gen::node_service::{ScannerScopedDirtyUsageAckRequest, ScannerScopedDirtyUsageAckResponse};
use prost::Message;

pub const SCOPED_DIRTY_USAGE_PROTOCOL_VERSION: u32 = 1;
pub const SCOPED_DIRTY_USAGE_BUCKET_SCOPE: u32 = 1;
pub const SCOPED_DIRTY_USAGE_MAX_ENTRIES: u32 = 32;
pub const SCOPED_DIRTY_USAGE_MAX_REQUEST_BYTES: u32 = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopedDirtyUsageRequestError {
    UnsupportedProtocol,
    UnsupportedScope,
    InvalidIdentity,
    InvalidGeneration,
    InvalidEntries,
    TooLarge,
}

impl std::fmt::Display for ScopedDirtyUsageRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::UnsupportedProtocol => "unsupported scoped dirty usage protocol",
            Self::UnsupportedScope => "unsupported scoped dirty usage scope",
            Self::InvalidIdentity => "invalid scoped dirty usage identity",
            Self::InvalidGeneration => "invalid scoped dirty usage generation",
            Self::InvalidEntries => "scoped dirty usage entries must be nonempty and strictly ordered",
            Self::TooLarge => "scoped dirty usage request exceeds its budget",
        })
    }
}

impl std::error::Error for ScopedDirtyUsageRequestError {}

pub fn validate_scoped_dirty_usage_request(
    request: &ScannerScopedDirtyUsageAckRequest,
) -> Result<(), ScopedDirtyUsageRequestError> {
    use ScopedDirtyUsageRequestError as E;
    if request.entries.len() > SCOPED_DIRTY_USAGE_MAX_ENTRIES as usize
        || request.encoded_len() > SCOPED_DIRTY_USAGE_MAX_REQUEST_BYTES as usize
    {
        return Err(E::TooLarge);
    }
    if request.protocol_version != SCOPED_DIRTY_USAGE_PROTOCOL_VERSION {
        return Err(E::UnsupportedProtocol);
    }
    if request.scope != SCOPED_DIRTY_USAGE_BUCKET_SCOPE {
        return Err(E::UnsupportedScope);
    }
    if request.challenge.len() != 16 || request.owner_id.len() != 36 || request.instance_id.len() != 32 {
        return Err(E::InvalidIdentity);
    }
    if request.entries.is_empty() || request.entries.windows(2).any(|pair| pair[0].bucket >= pair[1].bucket) {
        return Err(E::InvalidEntries);
    }
    for entry in &request.entries {
        if entry.bucket.is_empty()
            || entry.bucket.len() > 63
            || entry.bucket_incarnation.len() != 16
            || entry.bucket_incarnation.iter().all(|byte| *byte == 0)
        {
            return Err(E::InvalidIdentity);
        }
        if entry.generation == 0 || entry.generation == u64::MAX {
            return Err(E::InvalidGeneration);
        }
    }
    Ok(())
}

pub fn canonical_scoped_dirty_usage_request(
    request: &ScannerScopedDirtyUsageAckRequest,
) -> Result<Vec<u8>, ScopedDirtyUsageRequestError> {
    validate_scoped_dirty_usage_request(request)?;
    let mut body = CanonicalBodyBuilder::new(b"rustfs-scoped-dirty-usage-ack-request-v1\0");
    let encode = |_: std::num::TryFromIntError| ScopedDirtyUsageRequestError::TooLarge;
    body.push_bytes(request.challenge.as_ref()).map_err(encode)?;
    body.push_u32(request.protocol_version);
    body.push_str(&request.owner_id).map_err(encode)?;
    body.push_str(&request.instance_id).map_err(encode)?;
    body.push_u32(request.scope);
    body.push_bool(request.probe_only);
    body.push_count(request.entries.len()).map_err(encode)?;
    for entry in &request.entries {
        body.push_str(&entry.bucket).map_err(encode)?;
        body.push_bytes(entry.bucket_incarnation.as_ref()).map_err(encode)?;
        body.push_u64(entry.generation);
    }
    Ok(body.finish())
}

pub fn canonical_scoped_dirty_usage_response(
    request_body: &[u8],
    response: &ScannerScopedDirtyUsageAckResponse,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = CanonicalBodyBuilder::new(b"rustfs-scoped-dirty-usage-ack-response-v1\0");
    body.push_bytes(request_body)?;
    body.push_u32(response.protocol_version);
    body.push_str(&response.owner_id)?;
    body.push_str(&response.instance_id)?;
    body.push_bool(response.supported);
    body.push_u32(response.max_entries);
    body.push_u32(response.max_request_bytes);
    body.push_u64(response.cleared);
    Ok(body.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto_gen::node_service::ScannerScopedDirtyUsageEntry;

    fn request() -> ScannerScopedDirtyUsageAckRequest {
        ScannerScopedDirtyUsageAckRequest {
            challenge: vec![1; 16].into(),
            protocol_version: 1,
            owner_id: "11111111-1111-1111-1111-111111111111".into(),
            instance_id: "a".repeat(32),
            scope: 1,
            probe_only: false,
            entries: vec![ScannerScopedDirtyUsageEntry {
                bucket: "photos".into(),
                bucket_incarnation: vec![2; 16].into(),
                generation: 8,
            }],
        }
    }

    #[test]
    fn scoped_dirty_usage_binds_every_request_field() {
        let base = request();
        let baseline = canonical_scoped_dirty_usage_request(&base).expect("valid request");
        for field in 0..9 {
            let mut changed = base.clone();
            match field {
                0 => changed.challenge = vec![3; 16].into(),
                1 => changed.protocol_version += 1,
                2 => changed.owner_id = "22222222-2222-2222-2222-222222222222".into(),
                3 => changed.instance_id = "b".repeat(32),
                4 => changed.scope += 1,
                5 => changed.probe_only = true,
                6 => changed.entries[0].bucket = "videos".into(),
                7 => changed.entries[0].bucket_incarnation = vec![3; 16].into(),
                _ => changed.entries[0].generation += 1,
            }
            assert!(canonical_scoped_dirty_usage_request(&changed).map_or(true, |body| body != baseline));
        }
    }

    #[test]
    fn scoped_dirty_usage_binds_capability_and_ack_to_exact_request() {
        let request = canonical_scoped_dirty_usage_request(&request()).expect("valid request");
        let response = ScannerScopedDirtyUsageAckResponse {
            protocol_version: 1,
            owner_id: "owner".into(),
            instance_id: "process".into(),
            supported: true,
            max_entries: 32,
            max_request_bytes: 8192,
            cleared: 1,
            response_proof: vec![1; 32].into(),
        };
        let baseline = canonical_scoped_dirty_usage_response(&request, &response).expect("valid response");
        for field in 0..7 {
            let mut changed = response.clone();
            match field {
                0 => changed.protocol_version += 1,
                1 => changed.owner_id.push('x'),
                2 => changed.instance_id.push('x'),
                3 => changed.supported = false,
                4 => changed.max_entries += 1,
                5 => changed.max_request_bytes += 1,
                _ => changed.cleared += 1,
            }
            assert_ne!(
                canonical_scoped_dirty_usage_response(&request, &changed).expect("response variant"),
                baseline
            );
        }
        assert_ne!(
            canonical_scoped_dirty_usage_response(b"another request", &response).expect("request variant"),
            baseline
        );
    }

    #[test]
    fn scoped_dirty_usage_rejects_overflow_unknown_and_duplicate_entries() {
        let base = request();
        let mut invalid = base.clone();
        invalid.entries = vec![base.entries[0].clone(); SCOPED_DIRTY_USAGE_MAX_ENTRIES as usize + 1];
        assert_eq!(validate_scoped_dirty_usage_request(&invalid), Err(ScopedDirtyUsageRequestError::TooLarge));
        invalid = base.clone();
        invalid.entries[0].bucket = "x".repeat(SCOPED_DIRTY_USAGE_MAX_REQUEST_BYTES as usize);
        assert_eq!(validate_scoped_dirty_usage_request(&invalid), Err(ScopedDirtyUsageRequestError::TooLarge));
        invalid = base.clone();
        invalid.entries.push(base.entries[0].clone());
        assert_eq!(
            validate_scoped_dirty_usage_request(&invalid),
            Err(ScopedDirtyUsageRequestError::InvalidEntries)
        );
        invalid = base;
        invalid.entries[0].bucket_incarnation = vec![0; 16].into();
        assert_eq!(
            validate_scoped_dirty_usage_request(&invalid),
            Err(ScopedDirtyUsageRequestError::InvalidIdentity)
        );
    }
}
