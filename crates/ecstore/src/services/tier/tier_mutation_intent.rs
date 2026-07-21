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

use rustfs_utils::crypto::{hex_sha256, is_sha256_checksum};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::com;
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result as EcstoreResult};
use crate::services::tier::tier::TierDestinationId;
use crate::storage_api_contracts::list::ListOperations as _;
use crate::store::ECStore;

pub(crate) const TIER_MUTATION_INTENT_SCHEMA: &str = "rustfs-tier-mutation-intent-v1";
pub(crate) const MAX_TIER_MUTATION_INTENT_SIZE: usize = 64 * 1024;
pub(crate) const TIER_MUTATION_INTENT_RECORD_PREFIX: &str = "tier/mutation-intents/records";
pub(crate) type TierMutationDigest = [u8; 32];

pub(crate) type Result<T> = std::result::Result<T, TierMutationIntentError>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum TierMutationIntentError {
    #[error("tier mutation intent is corrupt: {0}")]
    Corrupt(&'static str),
    #[error("tier mutation intent schema is unsupported: {0}")]
    UnsupportedSchema(String),
    #[error("tier mutation intent checksum mismatch")]
    ChecksumMismatch,
    #[error("invalid tier mutation intent state change from {from:?} to {to:?}")]
    InvalidStateChange {
        from: TierMutationIntentState,
        to: TierMutationIntentState,
    },
    #[error("tier mutation intent json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TierMutationIntentState {
    Prepared,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TierMutationIntentKind {
    Add,
    Edit,
    Remove,
    Clear,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TierMutationIntentTarget {
    pub tier_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_backend_identity: Option<TierDestinationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_backend_identity: Option<TierDestinationId>,
}

impl TierMutationIntentTarget {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.tier_name.is_empty() {
            return Err(TierMutationIntentError::Corrupt("target tier name is empty"));
        }
        if self.old_backend_identity.is_none() && self.new_backend_identity.is_none() {
            return Err(TierMutationIntentError::Corrupt("target has no backend identity"));
        }
        if self.old_backend_identity.as_ref().is_some_and(identity_is_empty) {
            return Err(TierMutationIntentError::Corrupt("target old backend identity is empty"));
        }
        if self.new_backend_identity.as_ref().is_some_and(identity_is_empty) {
            return Err(TierMutationIntentError::Corrupt("target new backend identity is empty"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TierMutationIntent {
    pub mutation_id: Uuid,
    pub revision: u64,
    pub kind: TierMutationIntentKind,
    pub state: TierMutationIntentState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_config_etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_config_etag: Option<String>,
    pub candidate_digest: TierMutationDigest,
    pub affected_targets: Vec<TierMutationIntentTarget>,
    pub expires_at_unix_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TierMutationIntentRecordScan {
    pub intents: Vec<TierMutationIntent>,
    pub scanned: usize,
    pub failed: usize,
    pub next_marker: Option<String>,
    pub truncated: bool,
}

impl TierMutationIntent {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.mutation_id.is_nil() {
            return Err(TierMutationIntentError::Corrupt("mutation_id is nil"));
        }
        if self.revision == 0 {
            return Err(TierMutationIntentError::Corrupt("revision is zero"));
        }
        if self.old_config_etag.as_deref().is_some_and(str::is_empty) {
            return Err(TierMutationIntentError::Corrupt("old config etag is empty"));
        }
        if self.old_config_etag.is_none() && self.kind != TierMutationIntentKind::Add {
            return Err(TierMutationIntentError::Corrupt("old config etag is required for mutation kind"));
        }
        if self.committed_config_etag.as_deref().is_some_and(str::is_empty) {
            return Err(TierMutationIntentError::Corrupt("committed config etag is empty"));
        }
        if self.state != TierMutationIntentState::Committed && self.committed_config_etag.is_some() {
            return Err(TierMutationIntentError::Corrupt("non-committed intent carries committed config etag"));
        }
        if self.state == TierMutationIntentState::Committed && self.committed_config_etag.is_none() {
            return Err(TierMutationIntentError::Corrupt("committed intent is missing committed config etag"));
        }
        if digest_is_empty(&self.candidate_digest) {
            return Err(TierMutationIntentError::Corrupt("candidate digest is empty"));
        }
        if self.affected_targets.is_empty() {
            return Err(TierMutationIntentError::Corrupt("affected targets are empty"));
        }
        if self.expires_at_unix_nanos <= 0 {
            return Err(TierMutationIntentError::Corrupt("expiry timestamp is not positive"));
        }

        let mut previous: Option<&str> = None;
        for target in &self.affected_targets {
            target.validate()?;
            self.validate_target_shape(target)?;
            if previous.is_some_and(|previous| previous >= target.tier_name.as_str()) {
                return Err(TierMutationIntentError::Corrupt("affected targets are not canonical"));
            }
            previous = Some(&target.tier_name);
        }
        Ok(())
    }

    fn validate_target_shape(&self, target: &TierMutationIntentTarget) -> Result<()> {
        match self.kind {
            TierMutationIntentKind::Add if target.old_backend_identity.is_none() && target.new_backend_identity.is_some() => {
                Ok(())
            }
            TierMutationIntentKind::Edit if target.old_backend_identity.is_some() && target.new_backend_identity.is_some() => {
                Ok(())
            }
            TierMutationIntentKind::Remove | TierMutationIntentKind::Clear
                if target.old_backend_identity.is_some() && target.new_backend_identity.is_none() =>
            {
                Ok(())
            }
            _ => Err(TierMutationIntentError::Corrupt(
                "target backend identity shape does not match mutation kind",
            )),
        }
    }

    pub(crate) fn advance(&mut self, next: TierMutationIntentState, committed_config_etag: Option<String>) -> Result<()> {
        match (self.state, next) {
            (TierMutationIntentState::Prepared, TierMutationIntentState::Committed) => {
                let committed_config_etag =
                    committed_config_etag.ok_or(TierMutationIntentError::Corrupt("commit requires config etag"))?;
                if committed_config_etag.is_empty() {
                    return Err(TierMutationIntentError::Corrupt("commit config etag is empty"));
                }
                self.state = next;
                self.committed_config_etag = Some(committed_config_etag);
            }
            (TierMutationIntentState::Prepared, TierMutationIntentState::Aborted) => {
                if committed_config_etag.is_some() {
                    return Err(TierMutationIntentError::Corrupt("abort must not carry committed config etag"));
                }
                self.state = next;
                self.committed_config_etag = None;
            }
            _ => {
                return Err(TierMutationIntentError::InvalidStateChange {
                    from: self.state,
                    to: next,
                });
            }
        }
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(TierMutationIntentError::Corrupt("revision overflow"))?;
        self.validate()
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let intent_bytes = serde_json::to_vec(self)?;
        let content_sha256 = hex_sha256(&intent_bytes, ToOwned::to_owned);
        let persisted = PersistedTierMutationIntent {
            schema: TIER_MUTATION_INTENT_SCHEMA.to_string(),
            content_sha256,
            intent: self.clone(),
        };
        let encoded = serde_json::to_vec(&persisted)?;
        if encoded.len() > MAX_TIER_MUTATION_INTENT_SIZE {
            return Err(TierMutationIntentError::Corrupt("encoded intent exceeds maximum size"));
        }
        Ok(encoded)
    }

    pub(crate) fn decode(expected_mutation_id: Uuid, data: &[u8]) -> Result<Self> {
        if data.len() > MAX_TIER_MUTATION_INTENT_SIZE {
            return Err(TierMutationIntentError::Corrupt("encoded intent exceeds maximum size"));
        }
        let persisted: PersistedTierMutationIntent = serde_json::from_slice(data)?;
        if persisted.schema != TIER_MUTATION_INTENT_SCHEMA {
            return Err(TierMutationIntentError::UnsupportedSchema(persisted.schema));
        }
        if !is_sha256_checksum(&persisted.content_sha256) {
            return Err(TierMutationIntentError::Corrupt("content checksum is not a sha256 checksum"));
        }
        let intent_bytes = serde_json::to_vec(&persisted.intent)?;
        let actual_checksum = hex_sha256(&intent_bytes, ToOwned::to_owned);
        if persisted.content_sha256 != actual_checksum {
            return Err(TierMutationIntentError::ChecksumMismatch);
        }
        if persisted.intent.mutation_id != expected_mutation_id {
            return Err(TierMutationIntentError::Corrupt("mutation_id does not match intent key"));
        }
        persisted.intent.validate()?;
        Ok(persisted.intent)
    }
}

pub(crate) fn tier_mutation_intent_record_object_name(mutation_id: Uuid) -> Result<String> {
    if mutation_id.is_nil() {
        return Err(TierMutationIntentError::Corrupt("mutation_id is nil"));
    }
    let mutation_key = mutation_id.simple().to_string();
    Ok(format!(
        "{}/{}/{}/{}.json",
        TIER_MUTATION_INTENT_RECORD_PREFIX,
        &mutation_key[..2],
        &mutation_key[2..4],
        mutation_key
    ))
}

pub(crate) fn tier_mutation_intent_id_from_record_object_name(object: &str) -> Result<Uuid> {
    let prefix = format!("{TIER_MUTATION_INTENT_RECORD_PREFIX}/");
    let suffix = object
        .strip_prefix(&prefix)
        .ok_or(TierMutationIntentError::Corrupt("intent record path has wrong prefix"))?;
    let mut parts = suffix.split('/');
    let shard_a = parts
        .next()
        .ok_or(TierMutationIntentError::Corrupt("intent record path is incomplete"))?;
    let shard_b = parts
        .next()
        .ok_or(TierMutationIntentError::Corrupt("intent record path is incomplete"))?;
    let file_name = parts
        .next()
        .ok_or(TierMutationIntentError::Corrupt("intent record path is incomplete"))?;
    if parts.next().is_some() {
        return Err(TierMutationIntentError::Corrupt("intent record path is not canonical"));
    }
    let mutation_key = file_name
        .strip_suffix(".json")
        .ok_or(TierMutationIntentError::Corrupt("intent record path has wrong suffix"))?;
    if mutation_key.len() != 32
        || !mutation_key
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(TierMutationIntentError::Corrupt("intent record path has invalid mutation id"));
    }
    if shard_a != &mutation_key[..2] || shard_b != &mutation_key[2..4] {
        return Err(TierMutationIntentError::Corrupt("intent record path shard does not match mutation id"));
    }
    Uuid::parse_str(mutation_key).map_err(|_| TierMutationIntentError::Corrupt("intent record path has invalid uuid"))
}

pub(crate) async fn save_tier_mutation_intent_record(api: Arc<ECStore>, intent: &TierMutationIntent) -> EcstoreResult<()> {
    let object = tier_mutation_intent_record_object_name(intent.mutation_id).map_err(tier_mutation_intent_store_error)?;
    let data = intent.encode().map_err(tier_mutation_intent_store_error)?;
    com::save_config(api, &object, data).await
}

pub(crate) async fn load_tier_mutation_intent_record(api: Arc<ECStore>, mutation_id: Uuid) -> EcstoreResult<TierMutationIntent> {
    let object = tier_mutation_intent_record_object_name(mutation_id).map_err(tier_mutation_intent_store_error)?;
    let data = com::read_config(api, &object).await?;
    TierMutationIntent::decode(mutation_id, &data).map_err(tier_mutation_intent_store_error)
}

pub(crate) async fn delete_tier_mutation_intent_record(api: Arc<ECStore>, mutation_id: Uuid) -> EcstoreResult<()> {
    let object = tier_mutation_intent_record_object_name(mutation_id).map_err(tier_mutation_intent_store_error)?;
    match com::delete_config(api, &object).await {
        Ok(()) | Err(Error::ConfigNotFound) => Ok(()),
        Err(err) => Err(err),
    }
}

pub(crate) async fn list_tier_mutation_intent_records(
    api: Arc<ECStore>,
    limit: usize,
    marker: Option<String>,
) -> EcstoreResult<TierMutationIntentRecordScan> {
    if limit == 0 {
        return Err(Error::other("tier mutation intent scan limit must be greater than zero"));
    }

    let list = api
        .clone()
        .list_objects_v2(
            RUSTFS_META_BUCKET,
            TIER_MUTATION_INTENT_RECORD_PREFIX,
            marker,
            None,
            i32::try_from(limit).map_or(i32::MAX, |value| value),
            false,
            None,
            false,
        )
        .await?;
    let mut scan = TierMutationIntentRecordScan {
        intents: Vec::with_capacity(list.objects.len()),
        scanned: 0,
        failed: 0,
        next_marker: list.next_continuation_token,
        truncated: list.is_truncated,
    };

    for object in list.objects {
        scan.scanned += 1;
        let mutation_id = match tier_mutation_intent_id_from_record_object_name(&object.name) {
            Ok(mutation_id) => mutation_id,
            Err(_) => {
                scan.failed += 1;
                continue;
            }
        };
        match load_tier_mutation_intent_record(api.clone(), mutation_id).await {
            Ok(intent) => scan.intents.push(intent),
            Err(Error::ConfigNotFound) => {}
            Err(_) => scan.failed += 1,
        }
    }

    Ok(scan)
}

fn tier_mutation_intent_store_error(err: TierMutationIntentError) -> Error {
    Error::other(err)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedTierMutationIntent {
    schema: String,
    content_sha256: String,
    intent: TierMutationIntent,
}

fn identity_is_empty(identity: &TierDestinationId) -> bool {
    identity.iter().all(|byte| *byte == 0)
}

fn digest_is_empty(digest: &TierMutationDigest) -> bool {
    digest.iter().all(|byte| *byte == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const OLD_IDENTITY: TierDestinationId = [1; 32];
    const NEW_IDENTITY: TierDestinationId = [2; 32];
    const CANDIDATE_DIGEST: TierMutationDigest = [3; 32];

    fn prepared_intent() -> TierMutationIntent {
        TierMutationIntent {
            mutation_id: Uuid::new_v4(),
            revision: 1,
            kind: TierMutationIntentKind::Edit,
            state: TierMutationIntentState::Prepared,
            old_config_etag: Some("old-etag".to_string()),
            committed_config_etag: None,
            candidate_digest: CANDIDATE_DIGEST,
            affected_targets: vec![TierMutationIntentTarget {
                tier_name: "COLD-A".to_string(),
                old_backend_identity: Some(OLD_IDENTITY),
                new_backend_identity: Some(NEW_IDENTITY),
            }],
            expires_at_unix_nanos: 1_780_000_000_000_000_000,
        }
    }

    #[test]
    fn intent_round_trip_preserves_committed_state() {
        let mut intent = prepared_intent();
        intent
            .advance(TierMutationIntentState::Committed, Some("new-etag".to_string()))
            .expect("intent should commit");

        let encoded = intent.encode().expect("intent should encode");
        let decoded = TierMutationIntent::decode(intent.mutation_id, &encoded).expect("intent should decode");

        assert_eq!(decoded, intent);
        assert_eq!(decoded.committed_config_etag.as_deref(), Some("new-etag"));
    }

    #[test]
    fn intent_decode_rejects_unknown_fields_and_checksum_mismatch() {
        let intent = prepared_intent();
        let encoded = intent.encode().expect("intent should encode");
        let mut persisted: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded intent should be json");
        persisted["intent"]["unexpected"] = serde_json::Value::Bool(true);
        let unknown = serde_json::to_vec(&persisted).expect("mutated intent should encode");
        assert!(matches!(
            TierMutationIntent::decode(intent.mutation_id, &unknown),
            Err(TierMutationIntentError::Json(_))
        ));

        let mut persisted: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded intent should be json");
        persisted["intent"]["revision"] = serde_json::Value::from(2_u64);
        let mismatched = serde_json::to_vec(&persisted).expect("mutated intent should encode");
        assert!(matches!(
            TierMutationIntent::decode(intent.mutation_id, &mismatched),
            Err(TierMutationIntentError::ChecksumMismatch)
        ));
    }

    #[test]
    fn intent_validation_requires_canonical_targets() {
        let mut duplicate = prepared_intent();
        duplicate.affected_targets.push(TierMutationIntentTarget {
            tier_name: "COLD-A".to_string(),
            old_backend_identity: Some(OLD_IDENTITY),
            new_backend_identity: Some(NEW_IDENTITY),
        });
        assert!(matches!(
            duplicate.validate(),
            Err(TierMutationIntentError::Corrupt("affected targets are not canonical"))
        ));

        let mut unsorted = prepared_intent();
        unsorted.affected_targets.insert(
            0,
            TierMutationIntentTarget {
                tier_name: "COLD-Z".to_string(),
                old_backend_identity: Some(OLD_IDENTITY),
                new_backend_identity: Some(NEW_IDENTITY),
            },
        );
        assert!(matches!(
            unsorted.validate(),
            Err(TierMutationIntentError::Corrupt("affected targets are not canonical"))
        ));
    }

    #[test]
    fn intent_state_machine_rejects_late_abort_after_commit() {
        let mut intent = prepared_intent();
        intent
            .advance(TierMutationIntentState::Committed, Some("new-etag".to_string()))
            .expect("intent should commit");

        assert!(matches!(
            intent.advance(TierMutationIntentState::Aborted, None),
            Err(TierMutationIntentError::InvalidStateChange {
                from: TierMutationIntentState::Committed,
                to: TierMutationIntentState::Aborted,
            })
        ));
    }

    #[test]
    fn intent_validation_rejects_placeholder_identity() {
        let mut intent = prepared_intent();
        intent.affected_targets[0].old_backend_identity = Some([0; 32]);

        assert!(matches!(
            intent.validate(),
            Err(TierMutationIntentError::Corrupt("target old backend identity is empty"))
        ));
    }

    #[test]
    fn intent_allows_initial_config_create_without_old_etag() {
        let mut intent = prepared_intent();
        intent.kind = TierMutationIntentKind::Add;
        intent.old_config_etag = None;
        intent.affected_targets[0].old_backend_identity = None;

        let encoded = intent.encode().expect("initial create intent should encode");
        let decoded = TierMutationIntent::decode(intent.mutation_id, &encoded).expect("initial create intent should decode");

        assert_eq!(decoded.old_config_etag, None);
        assert_eq!(decoded.kind, TierMutationIntentKind::Add);
    }

    #[test]
    fn intent_record_object_name_is_canonical_and_reversible() {
        let mutation_id = Uuid::parse_str("12345678-1234-5678-9abc-def012345678").expect("test uuid should parse");

        let object = tier_mutation_intent_record_object_name(mutation_id).expect("record object should build");
        let parsed = tier_mutation_intent_id_from_record_object_name(&object).expect("record object should parse");

        assert_eq!(object, "tier/mutation-intents/records/12/34/12345678123456789abcdef012345678.json");
        assert_eq!(parsed, mutation_id);
    }

    #[test]
    fn intent_record_object_name_rejects_nil_and_malformed_paths() {
        assert!(matches!(
            tier_mutation_intent_record_object_name(Uuid::nil()),
            Err(TierMutationIntentError::Corrupt("mutation_id is nil"))
        ));
        assert!(matches!(
            tier_mutation_intent_id_from_record_object_name("tier/mutation-intents/records/12/34/not-a-uuid.json"),
            Err(TierMutationIntentError::Corrupt("intent record path has invalid mutation id"))
        ));
        assert!(matches!(
            tier_mutation_intent_id_from_record_object_name(
                "tier/mutation-intents/records/12/34/12345678123456789abcdef012345678"
            ),
            Err(TierMutationIntentError::Corrupt("intent record path has wrong suffix"))
        ));
        assert!(matches!(
            tier_mutation_intent_id_from_record_object_name(
                "tier/mutation-intents/records/12/35/12345678123456789abcdef012345678.json"
            ),
            Err(TierMutationIntentError::Corrupt("intent record path shard does not match mutation id"))
        ));
        assert!(matches!(
            tier_mutation_intent_id_from_record_object_name(
                "tier/mutation-intents/records/12/34/12345678123456789abcdef012345678/extra.json"
            ),
            Err(TierMutationIntentError::Corrupt("intent record path is not canonical"))
        ));
        assert!(matches!(
            tier_mutation_intent_id_from_record_object_name(
                "tier/mutation-intents/records/12/34/12345678123456789ABCDEF012345678.json"
            ),
            Err(TierMutationIntentError::Corrupt("intent record path has invalid mutation id"))
        ));
    }

    #[test]
    fn intent_decode_rejects_record_key_mismatch() {
        let intent = prepared_intent();
        let encoded = intent.encode().expect("intent should encode");
        let wrong_id = Uuid::new_v4();

        assert!(matches!(
            TierMutationIntent::decode(wrong_id, &encoded),
            Err(TierMutationIntentError::Corrupt("mutation_id does not match intent key"))
        ));
    }

    #[test]
    fn intent_validation_rejects_target_shape_mismatch() {
        let mut add = prepared_intent();
        add.kind = TierMutationIntentKind::Add;
        assert!(matches!(
            add.validate(),
            Err(TierMutationIntentError::Corrupt(
                "target backend identity shape does not match mutation kind"
            ))
        ));

        let mut remove = prepared_intent();
        remove.kind = TierMutationIntentKind::Remove;
        assert!(matches!(
            remove.validate(),
            Err(TierMutationIntentError::Corrupt(
                "target backend identity shape does not match mutation kind"
            ))
        ));

        let mut clear_without_etag = prepared_intent();
        clear_without_etag.kind = TierMutationIntentKind::Clear;
        clear_without_etag.old_config_etag = None;
        clear_without_etag.affected_targets[0].new_backend_identity = None;
        assert!(matches!(
            clear_without_etag.validate(),
            Err(TierMutationIntentError::Corrupt("old config etag is required for mutation kind"))
        ));
    }
}
