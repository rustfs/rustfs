// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Independently versioned, bounded bucket watch wire and authentication.

use crate::{
    CanonicalBodyBuilder,
    proto_gen::node_service::{ScannerUsageWatchRequest, ScannerUsageWatchResponse},
};
use prost::Message;

pub const USAGE_WATCH_PROTOCOL_VERSION: u32 = 1;
pub const USAGE_WATCH_MAX_ENTRIES: usize = 32;
pub const USAGE_WATCH_MAX_BYTES: usize = 8192;
pub const USAGE_WATCH_COVERAGE_VERSION: u32 = 1;
pub const USAGE_WATCH_MAX_LEASE_MS: u64 = 600_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageWatchRequestError {
    UnsupportedProtocol,
    InvalidIdentity,
    InvalidEntries,
    InvalidResponse,
    TooLarge,
}

impl std::fmt::Display for UsageWatchRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::UnsupportedProtocol => "unsupported usage watch protocol",
            Self::InvalidIdentity => "invalid usage watch identity",
            Self::InvalidEntries => "usage watch entries must be nonempty and strictly ordered",
            Self::InvalidResponse => "usage watch response does not match the requested proof",
            Self::TooLarge => "usage watch exceeds its resource budget",
        })
    }
}

/// Semantic checks are separate from authentication; the transport must
/// verify the proof over the canonical response before consuming any entry.
pub fn validate_usage_watch_response(
    request: &ScannerUsageWatchRequest,
    response: &ScannerUsageWatchResponse,
) -> Result<(), UsageWatchRequestError> {
    use UsageWatchRequestError as E;
    if response.encoded_len() > USAGE_WATCH_MAX_BYTES {
        return Err(E::TooLarge);
    }
    if response.protocol_version != USAGE_WATCH_PROTOCOL_VERSION
        || response.owner_id != request.owner_id
        || response.session_id.len() != 16
        || response.session_id.iter().all(|byte| *byte == 0)
        || response.entries.len() != request.entries.len()
    {
        return Err(E::InvalidResponse);
    }
    for (requested, entry) in request.entries.iter().zip(&response.entries) {
        if requested.bucket != entry.bucket {
            return Err(E::InvalidResponse);
        }
        if entry.known {
            if requested.release
                || (!requested.session_id.is_empty() && response.session_id != requested.session_id)
                || entry.bucket_incarnation != requested.bucket_incarnation
                || entry.watch_id.len() != 16
                || entry.watch_id.iter().all(|byte| *byte == 0)
                || (!requested.watch_id.is_empty() && entry.watch_id != requested.watch_id)
                || entry.revision == 0
                || entry.revision == u64::MAX
                || entry.inflight == u64::MAX
                || entry.coverage_version != USAGE_WATCH_COVERAGE_VERSION
                || entry.coverage_epoch == 0
                || entry.coverage_epoch == u64::MAX
                || entry.lease_remaining_ms == 0
                || entry.lease_remaining_ms > USAGE_WATCH_MAX_LEASE_MS
            {
                return Err(E::InvalidResponse);
            }
        } else if !entry.bucket_incarnation.is_empty()
            || !entry.watch_id.is_empty()
            || entry.revision != 0
            || entry.inflight != 0
            || entry.coverage_version != 0
            || entry.coverage_epoch != 0
            || entry.lease_remaining_ms != 0
        {
            return Err(E::InvalidResponse);
        }
    }
    Ok(())
}

impl std::error::Error for UsageWatchRequestError {}

pub fn canonical_usage_watch_request(request: &ScannerUsageWatchRequest) -> Result<Vec<u8>, UsageWatchRequestError> {
    use UsageWatchRequestError as E;
    if request.entries.len() > USAGE_WATCH_MAX_ENTRIES || request.encoded_len() > USAGE_WATCH_MAX_BYTES {
        return Err(E::TooLarge);
    }
    if request.protocol_version != USAGE_WATCH_PROTOCOL_VERSION {
        return Err(E::UnsupportedProtocol);
    }
    if request.challenge.len() != 16 || request.owner_id.len() != 36 {
        return Err(E::InvalidIdentity);
    }
    if request.entries.is_empty() || request.entries.windows(2).any(|pair| pair[0].bucket >= pair[1].bucket) {
        return Err(E::InvalidEntries);
    }
    let mut body = CanonicalBodyBuilder::new(b"rustfs-scanner-usage-watch-request-v1\0");
    let encode = |_: std::num::TryFromIntError| E::TooLarge;
    body.push_bytes(request.challenge.as_ref()).map_err(encode)?;
    body.push_u32(request.protocol_version);
    body.push_str(&request.owner_id).map_err(encode)?;
    body.push_count(request.entries.len()).map_err(encode)?;
    for entry in &request.entries {
        if entry.bucket.is_empty()
            || entry.bucket.len() > 63
            || matches!(entry.bucket.as_str(), "." | "..")
            || entry.bucket.chars().any(|ch| ch == '/' || ch == '\\' || ch.is_control())
            || entry.bucket_incarnation.len() != 16
            || entry.bucket_incarnation.iter().all(|byte| *byte == 0)
            || (!entry.watch_id.is_empty() && (entry.watch_id.len() != 16 || entry.watch_id.iter().all(|byte| *byte == 0)))
            || (!entry.session_id.is_empty() && (entry.session_id.len() != 16 || entry.session_id.iter().all(|byte| *byte == 0)))
            || (entry.release && (entry.watch_id.is_empty() || entry.session_id.is_empty()))
        {
            return Err(E::InvalidIdentity);
        }
        body.push_str(&entry.bucket).map_err(encode)?;
        body.push_bytes(entry.bucket_incarnation.as_ref()).map_err(encode)?;
        body.push_bytes(entry.watch_id.as_ref()).map_err(encode)?;
        body.push_bool(entry.release);
        body.push_bytes(entry.session_id.as_ref()).map_err(encode)?;
    }
    Ok(body.finish())
}

pub fn canonical_usage_watch_response(
    request: &[u8],
    response: &ScannerUsageWatchResponse,
) -> Result<Vec<u8>, UsageWatchRequestError> {
    use UsageWatchRequestError as E;
    if response.entries.len() > USAGE_WATCH_MAX_ENTRIES || response.encoded_len() > USAGE_WATCH_MAX_BYTES {
        return Err(E::TooLarge);
    }
    let encode = |_: std::num::TryFromIntError| E::TooLarge;
    let mut body = CanonicalBodyBuilder::new(b"rustfs-scanner-usage-watch-response-v1\0");
    body.push_bytes(request).map_err(encode)?;
    body.push_u32(response.protocol_version);
    body.push_str(&response.owner_id).map_err(encode)?;
    body.push_bytes(response.session_id.as_ref()).map_err(encode)?;
    body.push_count(response.entries.len()).map_err(encode)?;
    for entry in &response.entries {
        body.push_str(&entry.bucket).map_err(encode)?;
        body.push_bool(entry.known);
        body.push_bytes(entry.bucket_incarnation.as_ref()).map_err(encode)?;
        body.push_bytes(entry.watch_id.as_ref()).map_err(encode)?;
        body.push_u64(entry.revision);
        body.push_u64(entry.inflight);
        body.push_u32(entry.coverage_version);
        body.push_u64(entry.coverage_epoch);
        body.push_u64(entry.lease_remaining_ms);
    }
    Ok(body.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto_gen::node_service::{ScannerUsageWatchEntry, ScannerUsageWatchResult};

    fn request() -> ScannerUsageWatchRequest {
        ScannerUsageWatchRequest {
            challenge: vec![1; 16].into(),
            protocol_version: 1,
            owner_id: "11111111-1111-4111-8111-111111111111".into(),
            entries: vec![ScannerUsageWatchEntry {
                bucket: "bucket".into(),
                bucket_incarnation: vec![2; 16].into(),
                watch_id: vec![3; 16].into(),
                release: false,
                session_id: Vec::new().into(),
            }],
        }
    }

    fn response() -> ScannerUsageWatchResponse {
        ScannerUsageWatchResponse {
            protocol_version: 1,
            owner_id: request().owner_id,
            session_id: vec![4; 16].into(),
            entries: vec![ScannerUsageWatchResult {
                bucket: "bucket".into(),
                known: true,
                bucket_incarnation: vec![2; 16].into(),
                watch_id: vec![3; 16].into(),
                revision: 9,
                inflight: 0,
                coverage_version: 1,
                coverage_epoch: 1,
                lease_remaining_ms: 60_000,
            }],
            response_proof: vec![5; 32].into(),
        }
    }

    #[test]
    fn every_request_and_response_field_is_authenticated() {
        let base = request();
        let canonical = canonical_usage_watch_request(&base).expect("valid request");
        for field in 0..8 {
            let mut changed = base.clone();
            match field {
                0 => changed.challenge = vec![6; 16].into(),
                1 => changed.protocol_version += 1,
                2 => changed.owner_id = "22222222-2222-4222-8222-222222222222".into(),
                3 => changed.entries[0].bucket.push('x'),
                4 => changed.entries[0].bucket_incarnation = vec![7; 16].into(),
                5 => changed.entries[0].watch_id = vec![8; 16].into(),
                6 => {
                    changed.entries[0].release = true;
                    changed.entries[0].session_id = vec![4; 16].into();
                }
                _ => changed.entries[0].session_id = vec![8; 16].into(),
            }
            assert!(canonical_usage_watch_request(&changed).map_or(true, |value| value != canonical));
        }
        let base = response();
        let signed = canonical_usage_watch_response(&canonical, &base).expect("valid response");
        for field in 0..12 {
            let mut changed = base.clone();
            match field {
                0 => changed.protocol_version += 1,
                1 => changed.owner_id.push('x'),
                2 => changed.session_id = vec![8; 16].into(),
                3 => changed.entries[0].bucket.push('x'),
                4 => changed.entries[0].known = false,
                5 => changed.entries[0].bucket_incarnation = vec![8; 16].into(),
                6 => changed.entries[0].watch_id = vec![8; 16].into(),
                7 => changed.entries[0].revision += 1,
                8 => changed.entries[0].inflight += 1,
                9 => changed.entries[0].coverage_version += 1,
                10 => changed.entries[0].coverage_epoch += 1,
                _ => changed.entries[0].lease_remaining_ms += 1,
            }
            assert_ne!(canonical_usage_watch_response(&canonical, &changed).expect("response mutation"), signed);
        }
        assert_ne!(
            canonical_usage_watch_response(b"different request", &base).expect("other challenge"),
            signed
        );
    }

    #[test]
    fn missing_duplicate_future_or_forged_proofs_fail_closed() {
        let req = request();
        assert_eq!(validate_usage_watch_response(&req, &response()), Ok(()));
        for case in 0..13 {
            let mut changed = response();
            match case {
                0 => changed.entries.clear(),
                1 => changed.entries.push(changed.entries[0].clone()),
                2 => changed.session_id = vec![0; 16].into(),
                3 => changed.entries[0].watch_id = vec![8; 16].into(),
                4 => changed.entries[0].bucket_incarnation = vec![8; 16].into(),
                5 => changed.entries[0].revision = 0,
                6 => changed.entries[0].revision = u64::MAX,
                7 => changed.entries[0].coverage_version += 1,
                8 => changed.entries[0].coverage_epoch = 0,
                9 => changed.entries[0].lease_remaining_ms = 0,
                10 => changed.entries[0].lease_remaining_ms = USAGE_WATCH_MAX_LEASE_MS + 1,
                11 => changed.entries[0].known = false,
                _ => changed.owner_id.push('x'),
            }
            assert!(validate_usage_watch_response(&req, &changed).is_err(), "case {case}");
        }
        let mut unknown = response();
        unknown.entries[0] = ScannerUsageWatchResult {
            bucket: "bucket".into(),
            ..Default::default()
        };
        assert_eq!(validate_usage_watch_response(&req, &unknown), Ok(()));
        let mut overflow = req.clone();
        overflow.entries = vec![req.entries[0].clone(); USAGE_WATCH_MAX_ENTRIES + 1];
        assert_eq!(canonical_usage_watch_request(&overflow), Err(UsageWatchRequestError::TooLarge));
        overflow.entries.truncate(2);
        assert_eq!(canonical_usage_watch_request(&overflow), Err(UsageWatchRequestError::InvalidEntries));
    }

    #[test]
    fn release_requires_an_exact_token_and_session_and_never_returns_a_clean_proof() {
        let mut release = request();
        release.entries[0].release = true;
        assert!(canonical_usage_watch_request(&release).is_err());
        release.entries[0].session_id = vec![4; 16].into();
        assert!(canonical_usage_watch_request(&release).is_ok());
        assert!(validate_usage_watch_response(&release, &response()).is_err());
        let mut released = response();
        released.entries[0] = ScannerUsageWatchResult {
            bucket: "bucket".into(),
            ..Default::default()
        };
        assert_eq!(validate_usage_watch_response(&release, &released), Ok(()));
        release.entries[0].watch_id = Vec::new().into();
        assert!(canonical_usage_watch_request(&release).is_err());
        let mut refresh = request();
        refresh.entries[0].session_id = vec![9; 16].into();
        assert!(validate_usage_watch_response(&refresh, &response()).is_err());
    }

    #[test]
    fn watch_request_rejects_path_and_control_names() {
        for bucket in [
            ".",
            "..",
            "../bucket",
            "bucket/object",
            "bucket\\object",
            "bucket\n",
            "bucket\0",
        ] {
            let mut invalid = request();
            invalid.entries[0].bucket = bucket.into();
            assert!(canonical_usage_watch_request(&invalid).is_err());
        }
    }
}
