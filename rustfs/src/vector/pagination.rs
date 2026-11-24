// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Immutable pagination sessions shared by all nodes through Managed Object I/O.

use base64_simd::URL_SAFE_NO_PAD;
use rand::{TryRng as _, rngs::SysRng};
use rustfs_utils::crypto::{HmacSha256, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use super::protocol::ProtocolError;

pub(crate) const SESSION_TTL_SECONDS: i64 = 900;
pub(crate) const SESSION_CLOCK_ALLOWANCE_SECONDS: i64 = 60;
pub(crate) const QUERY_PAGE_SIZE: usize = 100;
pub(crate) const MAX_SESSION_BYTES: usize = 64 * 1024 * 1024;
const _: () = assert!(
    crate::table_catalog::VECTOR_QUERY_RESULTS_RETENTION_SECONDS > SESSION_TTL_SECONDS + 2 * SESSION_CLOCK_ALLOWANCE_SECONDS
);
const SESSION_MAGIC: &[u8; 8] = b"RFSVPG01";
const TOKEN_DOMAIN: &[u8] = b"rustfs.s3vectors.pagination.session.v1\0";

#[derive(Debug, thiserror::Error)]
pub(crate) enum PaginationError {
    #[error("invalid or expired pagination token")]
    InvalidToken,
    #[error("invalid persisted pagination session")]
    CorruptSession,
    #[error("pagination session exceeds its size budget")]
    CapacityExceeded,
    #[error("pagination session encoding failed")]
    Encoding(#[source] serde_json::Error),
    #[error("pagination session entropy is unavailable")]
    Entropy(#[source] std::io::Error),
}

impl PaginationError {
    pub(crate) fn invalid_token() -> ProtocolError {
        ProtocolError::validation("nextToken", "invalid or expired pagination token")
    }
}

/// Hashes bind canonical server-resolved identity and request facts without disclosing credentials.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionBinding {
    pub resource: [u8; 32],
    pub principal: [u8; 32],
    pub request: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredMatch {
    pub key: String,
    pub row_id: u64,
    pub distance_bits: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "operation", deny_unknown_fields)]
pub(crate) enum SessionResults {
    Query { matches: Vec<StoredMatch> },
    List,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionState {
    pub binding: SessionBinding,
    pub snapshot_version: u64,
    pub snapshot_digest: [u8; 32],
    pub issued_at: i64,
    pub expires_at: i64,
    pub results: SessionResults,
}

/// The signing key is deliberately absent from Debug and every client-visible representation.
pub(crate) struct PaginationSession {
    session_signing_key: Zeroizing<[u8; 32]>,
    pub state: SessionState,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenClaims {
    version: u8,
    session_id: String,
    state_digest: [u8; 32],
    binding: SessionBinding,
    snapshot_version: u64,
    snapshot_digest: [u8; 32],
    position: u64,
    issued_at: i64,
    expires_at: i64,
}

/// Parsing permits only a bounded, canonical identifier; no claim is trusted until verification.
pub(crate) struct ParsedToken {
    claims: TokenClaims,
    payload: Vec<u8>,
    tag: Vec<u8>,
}

impl ParsedToken {
    pub(crate) fn parse(value: &str) -> Result<Self, PaginationError> {
        if value.len() > 4096 {
            return Err(PaginationError::InvalidToken);
        }
        let (payload, tag) = value.split_once('.').ok_or(PaginationError::InvalidToken)?;
        let payload = URL_SAFE_NO_PAD
            .decode_to_vec(payload.as_bytes())
            .map_err(|_| PaginationError::InvalidToken)?;
        let tag = URL_SAFE_NO_PAD
            .decode_to_vec(tag.as_bytes())
            .map_err(|_| PaginationError::InvalidToken)?;
        let claims: TokenClaims = serde_json::from_slice(&payload).map_err(|_| PaginationError::InvalidToken)?;
        if claims.version != 1 || !valid_session_id(&claims.session_id) || tag.len() != 32 {
            return Err(PaginationError::InvalidToken);
        }
        Ok(Self { claims, payload, tag })
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.claims.session_id
    }
}

fn valid_session_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn digest(value: &impl Serialize) -> Result<[u8; 32], PaginationError> {
    let mut value = serde_json::to_value(value).map_err(PaginationError::Encoding)?;
    value.sort_all_objects();
    Ok(Sha256::digest(serde_json::to_vec(&value).map_err(PaginationError::Encoding)?).into())
}

impl PaginationSession {
    pub(crate) fn new(state: SessionState) -> Result<Self, PaginationError> {
        let mut session_signing_key = Zeroizing::new([0_u8; 32]);
        SysRng
            .try_fill_bytes(session_signing_key.as_mut())
            .map_err(|error| PaginationError::Entropy(std::io::Error::other(error)))?;
        let session = Self {
            session_signing_key,
            state,
        };
        session.validate()?;
        Ok(session)
    }

    pub(crate) fn id(&self) -> String {
        hex_simd::encode_to_string(Sha256::digest(self.session_signing_key.as_ref()), hex_simd::AsciiCase::Lower)
    }

    fn validate(&self) -> Result<(), PaginationError> {
        if self.state.snapshot_version == 0
            || self.state.issued_at < 0
            || self.state.issued_at.checked_add(SESSION_TTL_SECONDS) != Some(self.state.expires_at)
        {
            return Err(PaginationError::CorruptSession);
        }
        if let SessionResults::Query { matches } = &self.state.results
            && (matches.len() <= QUERY_PAGE_SIZE
                || matches.len() > 10_000
                || matches.iter().any(|item| {
                    item.key.is_empty()
                        || !f64::from_bits(item.distance_bits).is_finite()
                        || f64::from_bits(item.distance_bits) < 0.0
                }))
        {
            return Err(PaginationError::CorruptSession);
        }
        Ok(())
    }

    pub(crate) fn encode(&self) -> Result<Zeroizing<Vec<u8>>, PaginationError> {
        self.validate()?;
        let mut bytes = Zeroizing::new(Vec::new());
        bytes.extend_from_slice(SESSION_MAGIC);
        bytes.extend_from_slice(self.session_signing_key.as_ref());
        serde_json::to_writer(&mut *bytes, &self.state).map_err(PaginationError::Encoding)?;
        if bytes.len() > MAX_SESSION_BYTES {
            return Err(PaginationError::CapacityExceeded);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8], expected_id: &str) -> Result<Self, PaginationError> {
        if bytes.len() > MAX_SESSION_BYTES || bytes.len() <= 40 || &bytes[..8] != SESSION_MAGIC {
            return Err(PaginationError::CorruptSession);
        }
        let mut session_signing_key = Zeroizing::new([0_u8; 32]);
        session_signing_key.copy_from_slice(&bytes[8..40]);
        let state = serde_json::from_slice(&bytes[40..]).map_err(|_| PaginationError::CorruptSession)?;
        let session = Self {
            session_signing_key,
            state,
        };
        if session.id() != expected_id {
            return Err(PaginationError::CorruptSession);
        }
        session.validate()?;
        Ok(session)
    }

    pub(crate) fn token(&self, position: u64) -> Result<String, PaginationError> {
        let claims = TokenClaims {
            version: 1,
            session_id: self.id(),
            state_digest: digest(&self.state)?,
            binding: self.state.binding.clone(),
            snapshot_version: self.state.snapshot_version,
            snapshot_digest: self.state.snapshot_digest,
            position,
            issued_at: self.state.issued_at,
            expires_at: self.state.expires_at,
        };
        let payload = serde_json::to_vec(&claims).map_err(PaginationError::Encoding)?;
        let mut mac = <HmacSha256 as KeyInit>::new_from_slice(self.session_signing_key.as_ref())
            .map_err(|_| PaginationError::CorruptSession)?;
        mac.update(TOKEN_DOMAIN);
        mac.update(&payload);
        let tag = mac.finalize().into_bytes();
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode_to_string(payload),
            URL_SAFE_NO_PAD.encode_to_string(tag)
        ))
    }

    pub(crate) fn verify(&self, token: &ParsedToken, binding: &SessionBinding, now: i64) -> Result<u64, PaginationError> {
        let mut mac = <HmacSha256 as KeyInit>::new_from_slice(self.session_signing_key.as_ref())
            .map_err(|_| PaginationError::CorruptSession)?;
        mac.update(TOKEN_DOMAIN);
        mac.update(&token.payload);
        mac.verify_slice(&token.tag).map_err(|_| PaginationError::InvalidToken)?;
        let claims = &token.claims;
        if claims.session_id != self.id()
            || claims.state_digest != digest(&self.state)?
            || &claims.binding != binding
            || claims.binding != self.state.binding
            || claims.snapshot_version != self.state.snapshot_version
            || claims.snapshot_digest != self.state.snapshot_digest
            || claims.issued_at != self.state.issued_at
            || claims.expires_at != self.state.expires_at
            || now >= claims.expires_at
            || now.saturating_add(SESSION_CLOCK_ALLOWANCE_SECONDS) < claims.issued_at
        {
            return Err(PaginationError::InvalidToken);
        }
        Ok(claims.position)
    }
}

#[cfg(test)]
mod tests;
