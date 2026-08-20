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

use std::sync::Arc;

use rustfs_protos::{TIER_MUTATION_RPC_PROTOCOL_VERSION, TierMutationRpcPhase};
use uuid::Uuid;

use super::tier::{TierConfigMgr, tier_config_abort_matches, tier_config_commit_matches, tier_config_etag_matches};
use super::tier_mutation_intent::{
    MAX_TIER_MUTATION_INTENT_SIZE, TierMutationIntent, TierMutationIntentState, advance_tier_mutation_intent_record_idempotent,
    load_tier_mutation_intent_record, save_tier_mutation_intent_record_if_absent,
};
use crate::client::admin_handler_utils::AdminError;
use crate::error::{Error, StorageError};
use crate::store::ECStore;

pub const MAX_TIER_MUTATION_PEER_COMMIT_ETAG_SIZE: usize = rustfs_protos::TIER_MUTATION_RPC_MAX_COMMIT_PAYLOAD_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierMutationPeerState {
    Prepared,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierMutationPeerOutcome {
    pub state: TierMutationPeerState,
    pub applied: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum TierMutationPeerError {
    #[error("unsupported tier mutation peer protocol version: {0}")]
    UnsupportedProtocolVersion(u32),
    #[error("tier mutation peer mutation_id is nil")]
    NilMutationId,
    #[error("tier mutation peer payload is too large: {len}/{max}")]
    PayloadTooLarge { len: usize, max: usize },
    #[error("tier mutation peer payload is invalid: {0}")]
    InvalidPayload(String),
    #[error("tier mutation peer intent conflicts with existing record")]
    ConflictingIntent,
    #[error("tier mutation peer commit proof does not match the persisted tier configuration")]
    CommitProofMismatch,
    #[error("tier mutation peer abort proof does not match the persisted tier configuration")]
    AbortProofMismatch,
    #[error("tier mutation peer runtime error: {0}")]
    Runtime(#[source] AdminError),
    #[error("tier mutation peer store error: {0}")]
    Store(#[source] StorageError),
}

impl From<Error> for TierMutationPeerError {
    fn from(error: Error) -> Self {
        Self::Store(error)
    }
}

pub type TierMutationPeerResult<T> = std::result::Result<T, TierMutationPeerError>;

pub async fn handle_tier_mutation_peer_request(
    api: Arc<ECStore>,
    protocol_version: u32,
    phase: TierMutationRpcPhase,
    mutation_id: Uuid,
    canonical_payload: &[u8],
) -> TierMutationPeerResult<TierMutationPeerOutcome> {
    validate_peer_request_envelope(protocol_version, mutation_id, canonical_payload)?;
    match phase {
        TierMutationRpcPhase::Prepare => handle_prepare(api, mutation_id, canonical_payload).await,
        TierMutationRpcPhase::Commit => handle_commit(api, mutation_id, canonical_payload).await,
        TierMutationRpcPhase::Abort => handle_abort(api, mutation_id, canonical_payload).await,
        _ => Err(TierMutationPeerError::InvalidPayload(
            "tier mutation rpc phase is unsupported".to_string(),
        )),
    }
}

async fn handle_prepare(
    api: Arc<ECStore>,
    mutation_id: Uuid,
    canonical_payload: &[u8],
) -> TierMutationPeerResult<TierMutationPeerOutcome> {
    let intent = TierMutationIntent::decode(mutation_id, canonical_payload)
        .map_err(|err| TierMutationPeerError::InvalidPayload(err.to_string()))?;
    if intent.state != TierMutationIntentState::Prepared {
        return Err(TierMutationPeerError::InvalidPayload(
            "prepare intent must be in prepared state".to_string(),
        ));
    }
    let tier_config_mgr = api.tier_config_mgr();

    match save_tier_mutation_intent_record_if_absent(api.clone(), &intent).await {
        Ok(()) => {
            TierConfigMgr::apply_prepared_mutation_intent_block(&tier_config_mgr, &intent)
                .await
                .map_err(TierMutationPeerError::Runtime)?;
            Ok(TierMutationPeerOutcome {
                state: TierMutationPeerState::Prepared,
                applied: true,
            })
        }
        Err(Error::PreconditionFailed) => {
            let existing = load_tier_mutation_intent_record(api, mutation_id).await?;
            if !existing.same_identity_as(&intent) {
                return Err(TierMutationPeerError::ConflictingIntent);
            }
            match existing.state {
                TierMutationIntentState::Prepared => {
                    TierConfigMgr::apply_prepared_mutation_intent_block(&tier_config_mgr, &existing)
                        .await
                        .map_err(TierMutationPeerError::Runtime)?;
                }
                TierMutationIntentState::Committed => {
                    TierConfigMgr::apply_committed_mutation_intent_block(&tier_config_mgr, &existing)
                        .await
                        .map_err(TierMutationPeerError::Runtime)?;
                }
                TierMutationIntentState::Aborted => {
                    TierConfigMgr::request_committed_mutation_refresh(&tier_config_mgr).await;
                }
            }
            Ok(TierMutationPeerOutcome {
                state: peer_state_from_intent(existing.state),
                applied: false,
            })
        }
        Err(err) => Err(err.into()),
    }
}

async fn handle_commit(
    api: Arc<ECStore>,
    mutation_id: Uuid,
    canonical_payload: &[u8],
) -> TierMutationPeerResult<TierMutationPeerOutcome> {
    let committed_config_etag = parse_commit_etag(canonical_payload)?;
    let tier_config_mgr = api.tier_config_mgr();
    match load_tier_mutation_intent_record(api.clone(), mutation_id).await {
        Ok(intent) if intent.state == TierMutationIntentState::Prepared => {
            let proof_matches = tier_config_commit_matches(api.clone(), &committed_config_etag, intent.candidate_digest)
                .await
                .map_err(Error::other)?;
            if !proof_matches {
                return Err(TierMutationPeerError::CommitProofMismatch);
            }
        }
        Ok(_) | Err(Error::ConfigNotFound) => {}
        Err(err) => return Err(err.into()),
    }
    let (intent, applied) = match advance_tier_mutation_intent_record_idempotent(
        api.clone(),
        mutation_id,
        TierMutationIntentState::Committed,
        Some(committed_config_etag.clone()),
    )
    .await
    {
        Ok(result) => result,
        Err(Error::ConfigNotFound)
            if tier_config_etag_matches(api, &committed_config_etag)
                .await
                .map_err(Error::other)? =>
        {
            TierConfigMgr::promote_prepared_mutation_intent_block(&tier_config_mgr, mutation_id)
                .await
                .map_err(TierMutationPeerError::Runtime)?;
            return Ok(TierMutationPeerOutcome {
                state: TierMutationPeerState::Committed,
                applied: false,
            });
        }
        Err(err) => return Err(err.into()),
    };
    if intent.state == TierMutationIntentState::Committed {
        TierConfigMgr::apply_committed_mutation_intent_block(&tier_config_mgr, &intent)
            .await
            .map_err(TierMutationPeerError::Runtime)?;
    }
    Ok(TierMutationPeerOutcome {
        state: peer_state_from_intent(intent.state),
        applied,
    })
}

async fn handle_abort(
    api: Arc<ECStore>,
    mutation_id: Uuid,
    canonical_payload: &[u8],
) -> TierMutationPeerResult<TierMutationPeerOutcome> {
    if !canonical_payload.is_empty() {
        return Err(TierMutationPeerError::InvalidPayload("abort payload must be empty".to_string()));
    }
    let existing = load_tier_mutation_intent_record(api.clone(), mutation_id).await?;
    if existing.state == TierMutationIntentState::Prepared
        && !tier_config_abort_matches(api.clone(), &existing)
            .await
            .map_err(Error::other)?
    {
        return Err(TierMutationPeerError::AbortProofMismatch);
    }
    let (intent, applied) =
        advance_tier_mutation_intent_record_idempotent(api.clone(), mutation_id, TierMutationIntentState::Aborted, None).await?;
    if intent.state == TierMutationIntentState::Aborted {
        TierConfigMgr::request_committed_mutation_refresh(&api.tier_config_mgr()).await;
    }
    Ok(TierMutationPeerOutcome {
        state: peer_state_from_intent(intent.state),
        applied,
    })
}

fn validate_peer_request_envelope(
    protocol_version: u32,
    mutation_id: Uuid,
    canonical_payload: &[u8],
) -> TierMutationPeerResult<()> {
    if protocol_version != TIER_MUTATION_RPC_PROTOCOL_VERSION {
        return Err(TierMutationPeerError::UnsupportedProtocolVersion(protocol_version));
    }
    if mutation_id.is_nil() {
        return Err(TierMutationPeerError::NilMutationId);
    }
    if canonical_payload.len() > MAX_TIER_MUTATION_INTENT_SIZE {
        return Err(TierMutationPeerError::PayloadTooLarge {
            len: canonical_payload.len(),
            max: MAX_TIER_MUTATION_INTENT_SIZE,
        });
    }
    Ok(())
}

fn parse_commit_etag(canonical_payload: &[u8]) -> TierMutationPeerResult<String> {
    if canonical_payload.len() > MAX_TIER_MUTATION_PEER_COMMIT_ETAG_SIZE {
        return Err(TierMutationPeerError::PayloadTooLarge {
            len: canonical_payload.len(),
            max: MAX_TIER_MUTATION_PEER_COMMIT_ETAG_SIZE,
        });
    }
    let etag = std::str::from_utf8(canonical_payload)
        .map_err(|err| TierMutationPeerError::InvalidPayload(err.to_string()))?
        .trim();
    if etag.is_empty() {
        return Err(TierMutationPeerError::InvalidPayload(
            "commit payload must carry a committed config etag".to_string(),
        ));
    }
    Ok(etag.to_string())
}

fn peer_state_from_intent(state: TierMutationIntentState) -> TierMutationPeerState {
    match state {
        TierMutationIntentState::Prepared => TierMutationPeerState::Prepared,
        TierMutationIntentState::Committed => TierMutationPeerState::Committed,
        TierMutationIntentState::Aborted => TierMutationPeerState::Aborted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_request_envelope_fails_closed_on_old_version_nil_id_and_large_payload() {
        let mutation_id = Uuid::new_v4();
        assert!(matches!(
            validate_peer_request_envelope(TIER_MUTATION_RPC_PROTOCOL_VERSION + 1, mutation_id, b"payload"),
            Err(TierMutationPeerError::UnsupportedProtocolVersion(_))
        ));
        assert!(matches!(
            validate_peer_request_envelope(TIER_MUTATION_RPC_PROTOCOL_VERSION, Uuid::nil(), b"payload"),
            Err(TierMutationPeerError::NilMutationId)
        ));

        let oversized = vec![0; MAX_TIER_MUTATION_INTENT_SIZE + 1];
        assert!(matches!(
            validate_peer_request_envelope(TIER_MUTATION_RPC_PROTOCOL_VERSION, mutation_id, &oversized),
            Err(TierMutationPeerError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn commit_payload_requires_small_non_empty_utf8_etag() {
        assert_eq!(
            parse_commit_etag(b"  committed-etag  ").expect("etag payload should parse"),
            "committed-etag"
        );
        assert!(matches!(
            parse_commit_etag(b"   "),
            Err(TierMutationPeerError::InvalidPayload(message)) if message.contains("committed config etag")
        ));
        assert!(matches!(
            parse_commit_etag(&[0xff]),
            Err(TierMutationPeerError::InvalidPayload(message)) if message.contains("utf-8")
        ));

        let oversized = vec![b'a'; MAX_TIER_MUTATION_PEER_COMMIT_ETAG_SIZE + 1];
        assert!(matches!(
            parse_commit_etag(&oversized),
            Err(TierMutationPeerError::PayloadTooLarge { .. })
        ));
    }
}
