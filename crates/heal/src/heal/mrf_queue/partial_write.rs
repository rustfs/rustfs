// Copyright 2026 RustFS Team
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

use super::{
    HealManager, MrfConsumerConfig, MrfDurableRepairAnchor, MrfIntent, MrfLegacyRiskAcceptanceRequest, MrfQueueKey, queue_key,
    submit_mrf_heal_request,
};
use rustfs_common::mrf_channel::{MrfDurableAdmissionError, MrfIngressResult, release_mrf_intent, try_rearm_mrf_replay_intent};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Instant;
use uuid::Uuid;

const MAX_OPERATOR_REASON_BYTES: usize = 1024;
const MAX_OPERATOR_ACTOR_BYTES: usize = 256;
const MAX_OPERATOR_REFERENCE_BYTES: usize = 256;
const LIFECYCLE_CHECKPOINT_RECORD_OVERHEAD_BYTES: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PartialWriteKey {
    identity: MrfQueueKey,
    source_bucket_incarnation_id: Option<Uuid>,
}

impl PartialWriteKey {
    fn new(intent: &MrfIntent, source_bucket_incarnation_id: Option<Uuid>) -> Self {
        Self {
            identity: queue_key(intent),
            source_bucket_incarnation_id,
        }
    }

    fn from_anchor(anchor: &MrfDurableRepairAnchor) -> Self {
        Self {
            identity: MrfQueueKey {
                kind: anchor.kind,
                bucket: anchor.bucket.clone(),
                object: anchor.object.clone(),
                version_id: anchor.version_id,
                scope: anchor.scope,
                delete_marker_purge: anchor.delete_marker_purge,
            },
            source_bucket_incarnation_id: Some(anchor.bucket_incarnation_id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum ResponsibilityState {
    Active,
    HeldUnverifiedLegacy {
        bucket_incarnation_id: Uuid,
        since_ms: u64,
    },
    LegacyGenerationUnknown {
        observed_bucket_incarnation_id: Uuid,
        detected_at_ms: u64,
    },
    BucketIncarnationChanged {
        source_bucket_incarnation_id: Uuid,
        observed_bucket_incarnation_id: Uuid,
        detected_at_ms: u64,
    },
    OperatorAcceptedUnverified {
        bucket_incarnation_id: Uuid,
        acknowledged_unknown_source_incarnation: bool,
        acknowledged_incarnation_mismatch: bool,
        accepted_at_ms: u64,
        actor: String,
        reason: String,
        reference: String,
        request_id: Uuid,
    },
}

impl ResponsibilityState {
    fn is_parked(&self) -> bool {
        !matches!(self, Self::Active)
    }

    fn estimated_bytes(&self) -> usize {
        match self {
            Self::Active => 0,
            Self::HeldUnverifiedLegacy { .. } => std::mem::size_of::<Uuid>() + std::mem::size_of::<u64>(),
            Self::LegacyGenerationUnknown { .. } => std::mem::size_of::<Uuid>() + std::mem::size_of::<u64>(),
            Self::BucketIncarnationChanged { .. } => 2 * std::mem::size_of::<Uuid>() + std::mem::size_of::<u64>(),
            Self::OperatorAcceptedUnverified {
                actor,
                reason,
                reference,
                ..
            } => std::mem::size_of::<Uuid>() * 2 + std::mem::size_of::<u64>() + actor.len() + reason.len() + reference.len(),
        }
    }

    pub(super) fn bucket_incarnation_id(&self) -> Option<Uuid> {
        match self {
            Self::Active => None,
            Self::HeldUnverifiedLegacy {
                bucket_incarnation_id, ..
            }
            | Self::OperatorAcceptedUnverified {
                bucket_incarnation_id, ..
            } => Some(*bucket_incarnation_id),
            Self::LegacyGenerationUnknown {
                observed_bucket_incarnation_id,
                ..
            } => Some(*observed_bucket_incarnation_id),
            Self::BucketIncarnationChanged {
                observed_bucket_incarnation_id,
                ..
            } => Some(*observed_bucket_incarnation_id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ResponsibilityCheckpoint {
    pub intent_digest: [u8; 32],
    pub responsibility_id: Uuid,
    pub source_bucket_incarnation_id: Option<Uuid>,
    pub last_operator_acceptance: Option<MrfOperatorAcceptance>,
    pub state: ResponsibilityState,
}

impl ResponsibilityCheckpoint {
    pub(super) fn is_valid(&self) -> bool {
        if self.responsibility_id.is_nil() || self.source_bucket_incarnation_id.is_some_and(|value| value.is_nil()) {
            return false;
        }
        if self.last_operator_acceptance.as_ref().is_some_and(|audit| {
            validate_operator_audit_fields(&audit.actor, &audit.reason, &audit.reference, audit.request_id).is_err()
                || (self.source_bucket_incarnation_id.is_none() && !audit.acknowledged_unknown_source_incarnation)
        }) {
            return false;
        }
        match &self.state {
            ResponsibilityState::Active => true,
            ResponsibilityState::HeldUnverifiedLegacy {
                bucket_incarnation_id, ..
            } => !bucket_incarnation_id.is_nil() && self.source_bucket_incarnation_id == Some(*bucket_incarnation_id),
            ResponsibilityState::LegacyGenerationUnknown {
                observed_bucket_incarnation_id,
                ..
            } => !observed_bucket_incarnation_id.is_nil() && self.source_bucket_incarnation_id.is_none(),
            ResponsibilityState::BucketIncarnationChanged {
                source_bucket_incarnation_id,
                observed_bucket_incarnation_id,
                ..
            } => {
                !source_bucket_incarnation_id.is_nil()
                    && !observed_bucket_incarnation_id.is_nil()
                    && self.source_bucket_incarnation_id == Some(*source_bucket_incarnation_id)
            }
            ResponsibilityState::OperatorAcceptedUnverified {
                bucket_incarnation_id,
                acknowledged_unknown_source_incarnation,
                acknowledged_incarnation_mismatch,
                actor,
                reason,
                reference,
                request_id,
                ..
            } => {
                !bucket_incarnation_id.is_nil()
                    && self
                        .source_bucket_incarnation_id
                        .is_none_or(|source| source == *bucket_incarnation_id || *acknowledged_incarnation_mismatch)
                    && (self.source_bucket_incarnation_id.is_some() || *acknowledged_unknown_source_incarnation)
                    && validate_operator_audit_fields(actor, reason, reference, *request_id).is_ok()
            }
        }
    }
}

struct Responsibility {
    intent: MrfIntent,
    anchor: Option<MrfDurableRepairAnchor>,
    persisted: bool,
    responsibility_id: Uuid,
    source_bucket_incarnation_id: Option<Uuid>,
    last_operator_acceptance: Option<MrfOperatorAcceptance>,
    state: ResponsibilityState,
    retry_queued: bool,
    next_attempt: Instant,
}

/// Durable storage responsibilities have no guaranteed rediscovery producer.
/// Admission, task failure and retry exhaustion cannot release their records.
#[derive(Default)]
pub(super) struct PartialWrites {
    entries: HashMap<PartialWriteKey, Responsibility>,
    retry_order: VecDeque<PartialWriteKey>,
    retry_index: HashSet<PartialWriteKey>,
    bytes: usize,
}

impl PartialWrites {
    pub(super) fn cost(intent: &MrfIntent) -> usize {
        Self::cost_with_state(intent, &ResponsibilityState::Active)
    }

    pub(super) fn cost_with_state(intent: &MrfIntent, state: &ResponsibilityState) -> usize {
        intent.estimated_bytes()
            + std::mem::size_of::<Responsibility>()
            + 2 * std::mem::size_of::<PartialWriteKey>()
            + LIFECYCLE_CHECKPOINT_RECORD_OVERHEAD_BYTES
            + state.estimated_bytes()
    }

    pub(super) fn cost_with_state_and_audit(
        intent: &MrfIntent,
        state: &ResponsibilityState,
        audit: Option<&MrfOperatorAcceptance>,
    ) -> usize {
        Self::cost_with_state(intent, state)
            + audit.map_or(0, |audit| audit.actor.len() + audit.reason.len() + audit.reference.len())
    }

    fn entry_cost(entry: &Responsibility) -> usize {
        Self::cost_with_state_and_audit(&entry.intent, &entry.state, entry.last_operator_acceptance.as_ref())
    }

    #[cfg(test)]
    pub(super) fn admit(
        &mut self,
        intent: MrfIntent,
        capacity: usize,
        byte_budget: usize,
    ) -> Result<(), MrfDurableAdmissionError> {
        self.admit_with_source_incarnation(intent, None, capacity, byte_budget)
    }

    pub(super) fn admit_with_source_incarnation(
        &mut self,
        mut intent: MrfIntent,
        source_bucket_incarnation_id: Option<Uuid>,
        capacity: usize,
        byte_budget: usize,
    ) -> Result<(), MrfDurableAdmissionError> {
        if source_bucket_incarnation_id.is_some_and(|incarnation| incarnation.is_nil()) {
            return Err(MrfDurableAdmissionError::InvalidIdentity);
        }
        if try_rearm_mrf_replay_intent(&mut intent) != MrfIngressResult::Enqueued {
            return Err(MrfDurableAdmissionError::InvalidIdentity);
        }
        let key = PartialWriteKey::new(&intent, source_bucket_incarnation_id);
        let previous = self.entries.get(&key);
        let previous_was_held = previous.is_some_and(|entry| entry.state.is_parked());
        let mut retry_queued = previous.is_some_and(|entry| entry.retry_queued);
        let old_cost = previous.map_or(0, Self::entry_cost);
        let next_bytes = self.bytes.saturating_sub(old_cost).saturating_add(Self::cost(&intent));
        if (previous.is_none() && self.entries.len() >= capacity) || next_bytes > byte_budget {
            return Err(MrfDurableAdmissionError::Full);
        }
        if previous.is_some_and(|entry| entry.intent.lease == intent.lease) {
            return Ok(());
        }
        if previous.is_none() || (previous_was_held && !retry_queued) {
            if self.retry_index.insert(key.clone()) {
                self.retry_order.push_back(key.clone());
            }
            retry_queued = true;
        }
        self.entries.insert(
            key,
            Responsibility {
                intent,
                anchor: None,
                persisted: false,
                responsibility_id: Uuid::new_v4(),
                source_bucket_incarnation_id,
                last_operator_acceptance: None,
                state: ResponsibilityState::Active,
                retry_queued,
                next_attempt: Instant::now(),
            },
        );
        self.bytes = next_bytes;
        Ok(())
    }

    pub(super) fn depth(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn intents(&self) -> impl Iterator<Item = &MrfIntent> {
        self.entries.values().map(|entry| &entry.intent)
    }

    pub(super) fn anchors(&self) -> impl Iterator<Item = &MrfDurableRepairAnchor> {
        self.entries.values().filter_map(|entry| entry.anchor.as_ref())
    }

    pub(super) fn park_unverified_legacy(&mut self, anchor: &MrfDurableRepairAnchor) -> bool {
        let key = PartialWriteKey::from_anchor(anchor);
        let Some(entry) = self.entries.get_mut(&key) else {
            return false;
        };
        if entry.anchor.as_ref() != Some(anchor)
            || entry.state.is_parked()
            || anchor.bucket_incarnation_id.is_nil()
            || entry.source_bucket_incarnation_id != Some(anchor.bucket_incarnation_id)
        {
            return false;
        }
        let old_cost = Self::entry_cost(entry);
        entry.state = ResponsibilityState::HeldUnverifiedLegacy {
            bucket_incarnation_id: anchor.bucket_incarnation_id,
            since_ms: unix_now_ms(),
        };
        self.bytes = self.bytes.saturating_sub(old_cost).saturating_add(Self::entry_cost(entry));
        true
    }

    pub(super) fn unverified_legacy_count(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| matches!(&entry.state, ResponsibilityState::HeldUnverifiedLegacy { .. }))
            .count()
    }

    pub(super) fn bucket_incarnation_changed_count(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| matches!(&entry.state, ResponsibilityState::BucketIncarnationChanged { .. }))
            .count()
    }

    pub(super) fn legacy_generation_unknown_count(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| matches!(&entry.state, ResponsibilityState::LegacyGenerationUnknown { .. }))
            .count()
    }

    pub(super) fn operator_accepted_unverified_count(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| matches!(&entry.state, ResponsibilityState::OperatorAcceptedUnverified { .. }))
            .count()
    }

    pub(super) fn oldest_unverified_legacy_enqueued_at_ms(&self) -> Option<u64> {
        self.entries
            .values()
            .filter_map(|entry| match &entry.state {
                ResponsibilityState::HeldUnverifiedLegacy { since_ms, .. } => Some(*since_ms),
                _ => None,
            })
            .min()
    }

    pub(super) fn oldest_operator_accepted_at_ms(&self) -> Option<u64> {
        self.entries
            .values()
            .filter_map(|entry| match &entry.state {
                ResponsibilityState::OperatorAcceptedUnverified { accepted_at_ms, .. } => Some(*accepted_at_ms),
                _ => None,
            })
            .min()
    }

    pub(super) fn checkpoint_records(
        &self,
        digest_for: impl Fn(&MrfIntent) -> Option<[u8; 32]>,
    ) -> Vec<ResponsibilityCheckpoint> {
        self.entries
            .values()
            .filter_map(|entry| {
                Some(ResponsibilityCheckpoint {
                    intent_digest: digest_for(&entry.intent)?,
                    responsibility_id: entry.responsibility_id,
                    source_bucket_incarnation_id: entry.source_bucket_incarnation_id,
                    last_operator_acceptance: entry.last_operator_acceptance.clone(),
                    state: entry.state.clone(),
                })
            })
            .collect()
    }

    pub(super) fn restore_state(&mut self, intent: &MrfIntent, record: &ResponsibilityCheckpoint) -> bool {
        let key = PartialWriteKey::new(intent, record.source_bucket_incarnation_id);
        let Some(entry) = self.entries.get_mut(&key) else {
            return false;
        };
        if !matches!(&entry.state, ResponsibilityState::Active) {
            return false;
        }
        let old_cost = Self::entry_cost(entry);
        entry.responsibility_id = record.responsibility_id;
        entry.source_bucket_incarnation_id = record.source_bucket_incarnation_id;
        entry.last_operator_acceptance = record.last_operator_acceptance.clone();
        entry.state = record.state.clone();
        self.bytes = self.bytes.saturating_sub(old_cost).saturating_add(Self::entry_cost(entry));
        if entry.state.is_parked() {
            entry.retry_queued = false;
        }
        true
    }

    pub(super) fn intent_for_responsibility(
        &self,
        responsibility_id: Uuid,
    ) -> Option<(MrfIntent, ResponsibilityState, Option<Uuid>)> {
        self.entries
            .values()
            .find(|entry| entry.responsibility_id == responsibility_id)
            .map(|entry| (entry.intent.clone(), entry.state.clone(), entry.source_bucket_incarnation_id))
    }

    pub(super) fn is_active_responsibility(&self, responsibility_id: Uuid) -> bool {
        self.entries
            .values()
            .find(|entry| entry.responsibility_id == responsibility_id)
            .is_some_and(|entry| matches!(&entry.state, ResponsibilityState::Active))
    }

    pub(super) fn last_operator_acceptance(&self, responsibility_id: Uuid) -> Option<MrfOperatorAcceptance> {
        self.entries
            .values()
            .find(|entry| entry.responsibility_id == responsibility_id)
            .and_then(|entry| entry.last_operator_acceptance.clone())
    }

    pub(super) fn record_operator_acceptance(
        &mut self,
        byte_budget: usize,
        request: MrfLegacyRiskAcceptanceRequest,
    ) -> Result<(ResponsibilityState, Option<MrfOperatorAcceptance>), &'static str> {
        let MrfLegacyRiskAcceptanceRequest {
            responsibility_id,
            expected_bucket_incarnation_id,
            acknowledge_unknown_source_incarnation,
            acknowledge_incarnation_mismatch,
            actor,
            reason,
            reference,
            request_id,
        } = request;
        validate_operator_audit_fields(&actor, &reason, &reference, request_id)?;
        let entry = self
            .entries
            .values_mut()
            .find(|entry| entry.responsibility_id == responsibility_id)
            .ok_or("responsibility not found or generation changed")?;
        let (current_incarnation, has_incarnation_mismatch) = match &entry.state {
            ResponsibilityState::HeldUnverifiedLegacy {
                bucket_incarnation_id, ..
            } => (*bucket_incarnation_id, false),
            ResponsibilityState::LegacyGenerationUnknown {
                observed_bucket_incarnation_id,
                ..
            } => (*observed_bucket_incarnation_id, false),
            ResponsibilityState::BucketIncarnationChanged {
                observed_bucket_incarnation_id,
                ..
            } => (*observed_bucket_incarnation_id, true),
            ResponsibilityState::OperatorAcceptedUnverified {
                bucket_incarnation_id: existing_bucket_incarnation_id,
                actor: existing_actor,
                reason: existing_reason,
                reference: existing_reference,
                request_id: existing_request_id,
                acknowledged_unknown_source_incarnation: existing_unknown_ack,
                acknowledged_incarnation_mismatch: existing_mismatch_ack,
                ..
            } if *existing_request_id == request_id
                && *existing_bucket_incarnation_id == expected_bucket_incarnation_id
                && existing_actor == &actor
                && existing_reason == &reason
                && existing_reference == &reference
                && *existing_unknown_ack == acknowledge_unknown_source_incarnation
                && *existing_mismatch_ack == acknowledge_incarnation_mismatch =>
            {
                return Ok((entry.state.clone(), entry.last_operator_acceptance.clone()));
            }
            ResponsibilityState::OperatorAcceptedUnverified {
                request_id: existing_request_id,
                ..
            } if *existing_request_id == request_id => return Err("request ID was reused with different audit fields"),
            ResponsibilityState::OperatorAcceptedUnverified { .. } => {
                return Err("a different operator disposition already exists");
            }
            ResponsibilityState::Active => return Err("responsibility is not held as unverified legacy"),
        };
        if current_incarnation != expected_bucket_incarnation_id {
            return Err("bucket incarnation changed; refresh the responsibility listing");
        }
        if entry.source_bucket_incarnation_id.is_none() && !acknowledge_unknown_source_incarnation {
            return Err("the original bucket incarnation is unknown; explicit acknowledgment is required");
        }
        if has_incarnation_mismatch && !acknowledge_incarnation_mismatch {
            return Err("the source and current bucket incarnations differ; explicit acknowledgment is required");
        }
        let previous = entry.state.clone();
        let previous_acceptance = entry.last_operator_acceptance.clone();
        let old_cost = Self::entry_cost(entry);
        let accepted_at_ms = unix_now_ms();
        let acceptance = MrfOperatorAcceptance {
            accepted_at_ms,
            actor: actor.clone(),
            reason: reason.clone(),
            reference: reference.clone(),
            request_id,
            acknowledged_unknown_source_incarnation: acknowledge_unknown_source_incarnation,
            acknowledged_incarnation_mismatch: acknowledge_incarnation_mismatch,
        };
        let next_state = ResponsibilityState::OperatorAcceptedUnverified {
            bucket_incarnation_id: current_incarnation,
            acknowledged_unknown_source_incarnation: acknowledge_unknown_source_incarnation,
            acknowledged_incarnation_mismatch: acknowledge_incarnation_mismatch,
            accepted_at_ms,
            actor,
            reason,
            reference,
            request_id,
        };
        let next_cost = Self::cost_with_state_and_audit(&entry.intent, &next_state, Some(&acceptance));
        let next_bytes = self.bytes.saturating_sub(old_cost).saturating_add(next_cost);
        if next_bytes > byte_budget {
            return Err("durable responsibility byte budget is exhausted");
        }
        entry.state = next_state;
        entry.last_operator_acceptance = Some(acceptance);
        self.bytes = next_bytes;
        Ok((previous, previous_acceptance))
    }

    pub(super) fn recheck(
        &mut self,
        responsibility_id: Uuid,
        expected_bucket_incarnation_id: Uuid,
    ) -> Result<(MrfIntent, ResponsibilityState), &'static str> {
        let entry = self
            .entries
            .values_mut()
            .find(|entry| entry.responsibility_id == responsibility_id)
            .ok_or("responsibility not found or generation changed")?;
        let current_incarnation = entry
            .state
            .bucket_incarnation_id()
            .ok_or("responsibility is not held as unverified legacy")?;
        if entry.source_bucket_incarnation_id.is_none()
            || matches!(&entry.state, ResponsibilityState::LegacyGenerationUnknown { .. })
        {
            return Err("the source bucket incarnation is unknown; targeted recheck is not safe");
        }
        if entry.source_bucket_incarnation_id != Some(expected_bucket_incarnation_id) {
            return Err("source and current bucket incarnations differ; targeted recheck is not safe");
        }
        if matches!(&entry.state, ResponsibilityState::BucketIncarnationChanged { .. }) {
            return Err("source and current bucket incarnations differ; recheck is not safe");
        }
        if matches!(&entry.state, ResponsibilityState::LegacyGenerationUnknown { .. }) {
            return Err("source bucket incarnation is unknown; automatic recheck is not safe");
        }
        if current_incarnation != expected_bucket_incarnation_id {
            return Err("bucket incarnation changed; refresh the responsibility listing");
        }
        let previous = entry.state.clone();
        let old_cost = Self::entry_cost(entry);
        entry.state = ResponsibilityState::Active;
        self.bytes = self.bytes.saturating_sub(old_cost).saturating_add(Self::entry_cost(entry));
        entry.persisted = false;
        entry.next_attempt = Instant::now();
        if !entry.retry_queued {
            let key = PartialWriteKey::new(&entry.intent, entry.source_bucket_incarnation_id);
            if self.retry_index.insert(key.clone()) {
                self.retry_order.push_back(key);
            }
            entry.retry_queued = true;
        }
        Ok((entry.intent.clone(), previous))
    }

    pub(super) fn restore_operator_state(
        &mut self,
        responsibility_id: Uuid,
        state: ResponsibilityState,
        last_operator_acceptance: Option<MrfOperatorAcceptance>,
    ) -> bool {
        let Some(entry) = self
            .entries
            .values_mut()
            .find(|entry| entry.responsibility_id == responsibility_id)
        else {
            return false;
        };
        let old_cost = Self::entry_cost(entry);
        entry.state = state;
        entry.last_operator_acceptance = last_operator_acceptance;
        self.bytes = self.bytes.saturating_sub(old_cost).saturating_add(Self::entry_cost(entry));
        entry.persisted = true;
        if entry.state.is_parked() {
            entry.retry_queued = false;
        }
        true
    }

    pub(super) fn list_unverified_legacy(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> (Vec<MrfLegacyResponsibility>, Option<Uuid>) {
        let mut selected = BTreeMap::new();
        for entry in self.entries.values() {
            let (status, bucket_incarnation_id, held_since_ms) = match &entry.state {
                ResponsibilityState::Active => continue,
                ResponsibilityState::HeldUnverifiedLegacy {
                    bucket_incarnation_id,
                    since_ms,
                } => ("held_unverified_legacy", *bucket_incarnation_id, Some(*since_ms)),
                ResponsibilityState::LegacyGenerationUnknown {
                    observed_bucket_incarnation_id,
                    detected_at_ms,
                } => ("legacy_generation_unknown", *observed_bucket_incarnation_id, Some(*detected_at_ms)),
                ResponsibilityState::BucketIncarnationChanged {
                    observed_bucket_incarnation_id,
                    detected_at_ms,
                    ..
                } => ("bucket_incarnation_changed", *observed_bucket_incarnation_id, Some(*detected_at_ms)),
                ResponsibilityState::OperatorAcceptedUnverified {
                    bucket_incarnation_id, ..
                } => ("operator_accepted_unverified", *bucket_incarnation_id, None),
            };
            if after.is_some_and(|after| entry.responsibility_id <= after) {
                continue;
            }
            if selected.len() > limit
                && selected
                    .last_key_value()
                    .is_some_and(|(largest_selected, _)| entry.responsibility_id >= *largest_selected)
            {
                continue;
            }
            let row = MrfLegacyResponsibility {
                responsibility_id: entry.responsibility_id,
                bucket: entry.intent.bucket.to_string(),
                object: entry.intent.object.to_string(),
                source_bucket_incarnation_id: entry.source_bucket_incarnation_id,
                version_id: entry
                    .intent
                    .version_id
                    .and_then(|bytes| Uuid::from_slice(&bytes).ok())
                    .filter(|version| !version.is_nil())
                    .map(|version| version.to_string()),
                pool_index: entry.intent.scope.map(|scope| scope.pool_index),
                set_index: entry.intent.scope.map(|scope| scope.set_index),
                bucket_incarnation_id,
                enqueued_at_ms: entry.intent.enqueued_at_ms,
                status,
                held_since_ms,
                accepted: entry.last_operator_acceptance.clone(),
            };
            selected.insert(entry.responsibility_id, row);
            if selected.len() > limit + 1 {
                selected.pop_last();
            }
        }
        let next_cursor = if selected.len() > limit {
            selected.keys().nth(limit - 1).copied()
        } else {
            None
        };
        if selected.len() > limit {
            selected.pop_last();
        }
        (selected.into_values().collect(), next_cursor)
    }

    pub(super) fn replace_observed_incarnation(
        &mut self,
        responsibility_id: Uuid,
        expected_bucket_incarnation_id: Uuid,
        observed_bucket_incarnation_id: Uuid,
        source_bucket_incarnation_id: Option<Uuid>,
    ) -> Result<(ResponsibilityState, Option<MrfOperatorAcceptance>), &'static str> {
        let entry = self
            .entries
            .values_mut()
            .find(|entry| entry.responsibility_id == responsibility_id)
            .ok_or("responsibility not found or generation changed")?;
        if entry.state.bucket_incarnation_id() != Some(expected_bucket_incarnation_id) {
            return Err("responsibility changed; refresh the listing");
        }
        let previous = entry.state.clone();
        let previous_acceptance = entry.last_operator_acceptance.clone();
        if source_bucket_incarnation_id.is_some_and(|source| source != observed_bucket_incarnation_id) {
            let old_cost = Self::entry_cost(entry);
            entry.state = ResponsibilityState::BucketIncarnationChanged {
                source_bucket_incarnation_id: match source_bucket_incarnation_id {
                    Some(source) => source,
                    None => return Err("source incarnation changed while refreshing responsibility"),
                },
                observed_bucket_incarnation_id,
                detected_at_ms: unix_now_ms(),
            };
            self.bytes = self.bytes.saturating_sub(old_cost).saturating_add(Self::entry_cost(entry));
            entry.retry_queued = false;
        } else if source_bucket_incarnation_id.is_none() {
            let old_cost = Self::entry_cost(entry);
            entry.state = ResponsibilityState::LegacyGenerationUnknown {
                observed_bucket_incarnation_id,
                detected_at_ms: unix_now_ms(),
            };
            self.bytes = self.bytes.saturating_sub(old_cost).saturating_add(Self::entry_cost(entry));
            entry.retry_queued = false;
        }
        Ok((previous, previous_acceptance))
    }

    pub(super) fn mark_persisted(&mut self) {
        for entry in self.entries.values_mut() {
            entry.persisted = true;
        }
    }

    pub(super) async fn dispatch_collecting_lifecycle_change(
        &mut self,
        manager: &HealManager,
        config: &MrfConsumerConfig,
    ) -> bool {
        let now = Instant::now();
        let mut lifecycle_changed = false;
        for key in self.ready_keys(now, config.replay_batch) {
            let Some(entry) = self.entries.get_mut(&key) else {
                continue;
            };
            entry.next_attempt = now + config.admission_backoff;
            // The bucket can be deleted and recreated while an obligation is
            // parked, so every dispatch must compare against a fresh identity.
            entry.anchor = manager.durable_mrf_repair_anchor(&entry.intent).await;
            if let Some(anchor) = entry.anchor.clone() {
                if entry.source_bucket_incarnation_id.is_none() {
                    let old_cost = Self::entry_cost(entry);
                    let detected_at_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
                    entry.state = ResponsibilityState::LegacyGenerationUnknown {
                        observed_bucket_incarnation_id: anchor.bucket_incarnation_id,
                        detected_at_ms,
                    };
                    self.bytes = self.bytes.saturating_sub(old_cost).saturating_add(Self::entry_cost(entry));
                    entry.retry_queued = false;
                    lifecycle_changed = true;
                    continue;
                }
                if let Some(source_bucket_incarnation_id) = entry.source_bucket_incarnation_id
                    && source_bucket_incarnation_id != anchor.bucket_incarnation_id
                {
                    let old_cost = Self::entry_cost(entry);
                    let detected_at_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
                    entry.state = ResponsibilityState::BucketIncarnationChanged {
                        source_bucket_incarnation_id,
                        observed_bucket_incarnation_id: anchor.bucket_incarnation_id,
                        detected_at_ms,
                    };
                    self.bytes = self.bytes.saturating_sub(old_cost).saturating_add(Self::entry_cost(entry));
                    entry.retry_queued = false;
                    lifecycle_changed = true;
                    continue;
                }
                // Proofless legacy results stay owned by the durable journal;
                // the lifecycle checkpoint determines whether this process
                // retries or waits for an explicit operator action.
                let _ = submit_mrf_heal_request(manager, &entry.intent, Some(anchor)).await;
            }
        }
        lifecycle_changed
    }

    fn ready_keys(&mut self, now: Instant, limit: usize) -> Vec<PartialWriteKey> {
        let mut ready = Vec::with_capacity(limit.min(self.entries.len()));
        // Rotation prevents a permanently failing prefix from starving the
        // rest of a backlog larger than one retry interval's batch budget.
        for _ in 0..self.retry_order.len() {
            if ready.len() == limit {
                break;
            }
            let Some(key) = self.retry_order.pop_front() else {
                break;
            };
            self.retry_index.remove(&key);
            let Some(entry) = self.entries.get_mut(&key) else {
                continue;
            };
            entry.retry_queued = false;
            if entry.state.is_parked() {
                // Held obligations remain in the durable responsibility map,
                // but leave the hot retry index until a new generation arrives.
                continue;
            }
            let due = entry.persisted && entry.next_attempt <= now;
            entry.retry_queued = true;
            self.retry_index.insert(key.clone());
            self.retry_order.push_back(key.clone());
            if due {
                ready.push(key);
            }
        }
        ready
    }

    pub(super) fn retain_unproven(&mut self, remaining: &HashSet<MrfDurableRepairAnchor>) -> bool {
        let before = self.entries.len();
        self.entries.retain(|_, entry| {
            let keep = entry.anchor.as_ref().is_none_or(|anchor| remaining.contains(anchor));
            if !keep {
                release_mrf_intent(&entry.intent);
            }
            keep
        });
        if before == self.entries.len() {
            return false;
        }
        self.bytes = self.entries.values().map(Self::entry_cost).sum();
        self.retry_order.retain(|key| self.entries.contains_key(key));
        self.retry_index.retain(|key| self.entries.contains_key(key));
        true
    }
}

fn validate_operator_audit_fields(actor: &str, reason: &str, reference: &str, request_id: Uuid) -> Result<(), &'static str> {
    if request_id.is_nil() || actor.trim().is_empty() || actor.len() > MAX_OPERATOR_ACTOR_BYTES {
        return Err("operator identity or request id is invalid");
    }
    if reason.trim().is_empty() || reason.len() > MAX_OPERATOR_REASON_BYTES {
        return Err("a reason of at most 1024 bytes is required");
    }
    if reference.trim().is_empty() || reference.len() > MAX_OPERATOR_REFERENCE_BYTES {
        return Err("an audit reference of at most 256 bytes is required");
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MrfLegacyResponsibility {
    pub responsibility_id: Uuid,
    pub bucket: String,
    pub object: String,
    pub source_bucket_incarnation_id: Option<Uuid>,
    pub version_id: Option<String>,
    pub pool_index: Option<u32>,
    pub set_index: Option<u32>,
    pub bucket_incarnation_id: Uuid,
    pub enqueued_at_ms: u64,
    pub status: &'static str,
    pub held_since_ms: Option<u64>,
    pub accepted: Option<MrfOperatorAcceptance>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MrfOperatorAcceptance {
    pub accepted_at_ms: u64,
    pub actor: String,
    pub reason: String,
    pub reference: String,
    pub request_id: Uuid,
    pub acknowledged_unknown_source_incarnation: bool,
    pub acknowledged_incarnation_mismatch: bool,
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heal::mrf_queue::MrfLegacyRiskAcceptanceRequest;
    use crate::heal::storage::{ECStoreHealStorage, HealStorageAPI};
    use rustfs_common::mrf_channel::{MrfKind, MrfScope, MrfVerifiedRepairDisposition, MrfVerifiedRepairEvent};
    use serial_test::serial;
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    fn intent(object: &str) -> MrfIntent {
        let mut intent = MrfIntent {
            bucket: Arc::from("partial-write-retention"),
            object: Arc::from(object),
            version_id: None,
            kind: MrfKind::PartialWrite,
            delete_marker_purge: None,
            scope: Some(MrfScope {
                pool_index: 0,
                set_index: 0,
            }),
            lease: None,
            enqueued_at_ms: 1,
            attempts: 0,
        };
        assert_eq!(try_rearm_mrf_replay_intent(&mut intent), MrfIngressResult::Enqueued);
        intent
    }

    fn risk_acceptance(
        responsibility_id: Uuid,
        expected_bucket_incarnation_id: Uuid,
        acknowledge_unknown_source_incarnation: bool,
        acknowledge_incarnation_mismatch: bool,
        reason: &str,
        reference: &str,
    ) -> MrfLegacyRiskAcceptanceRequest {
        MrfLegacyRiskAcceptanceRequest {
            responsibility_id,
            expected_bucket_incarnation_id,
            acknowledge_unknown_source_incarnation,
            acknowledge_incarnation_mismatch,
            actor: "operator-a".to_string(),
            reason: reason.to_string(),
            reference: reference.to_string(),
            request_id: Uuid::new_v4(),
        }
    }

    #[test]
    fn partial_write_retention_bounds_count_and_bytes_without_evicting_responsibility() {
        let first = intent("a");
        let cost = PartialWrites::cost(&first);
        let mut writes = PartialWrites::default();
        assert_eq!(writes.admit(first.clone(), 1, cost - 1), Err(MrfDurableAdmissionError::Full));
        writes
            .admit(first.clone(), 1, cost)
            .expect("exact byte and count boundary should fit");
        assert_eq!(writes.admit(intent("b"), 1, cost * 2), Err(MrfDurableAdmissionError::Full));
        assert_eq!(writes.admit(intent("b"), 2, cost), Err(MrfDurableAdmissionError::Full));
        assert_eq!(writes.depth(), 1);
        assert_eq!(writes.bytes(), cost);
        assert_eq!(writes.intents().next().expect("resident intent must remain").lease, first.lease);
    }

    #[test]
    fn partial_write_retention_retries_rotate_past_a_failing_prefix() {
        let mut writes = PartialWrites::default();
        for object in ["a", "b", "c"] {
            writes.admit(intent(object), 3, 8192).expect("bounded backlog should fit");
        }
        let now = Instant::now();
        assert!(writes.ready_keys(now, 2).is_empty(), "uncommitted responsibility must not be dispatched");
        writes.mark_persisted();
        let first: Vec<_> = writes.ready_keys(now, 2).into_iter().map(|key| key.identity.object).collect();
        assert_eq!(first, vec![Arc::<str>::from("a"), Arc::<str>::from("b")]);
        let second = writes.ready_keys(now, 2);
        assert_eq!(
            second[0].identity.object.as_ref(),
            "c",
            "the old prefix becoming due again cannot starve the next member"
        );
    }

    #[test]
    fn unverified_legacy_without_matching_lifecycle_checkpoint_rechecks_after_restart() {
        let original = intent("legacy");
        let incarnation = Uuid::new_v4();
        let mut writes = PartialWrites::default();
        writes
            .admit_with_source_incarnation(original.clone(), Some(incarnation), 1, 8192)
            .expect("durable intent should be retained");
        writes.mark_persisted();
        let anchor = MrfDurableRepairAnchor::from_intent(&original, incarnation).expect("exact durable anchor");
        writes
            .entries
            .get_mut(&PartialWriteKey::new(&original, Some(incarnation)))
            .expect("resident intent")
            .anchor = Some(anchor.clone());

        assert!(writes.park_unverified_legacy(&anchor));
        assert!(!writes.park_unverified_legacy(&anchor), "duplicate notices are idempotent");
        assert_eq!(writes.unverified_legacy_count(), 1);
        assert_eq!(writes.depth(), 1, "holding the intent must preserve responsibility");
        assert!(writes.ready_keys(Instant::now() + Duration::from_secs(60), 1).is_empty());
        assert!(writes.retry_order.is_empty(), "held intents leave the retry index after one pass");

        let replacement = intent("legacy");
        writes
            .admit_with_source_incarnation(replacement, Some(incarnation), 1, 8192)
            .expect("a new generation should become retryable");
        writes.mark_persisted();
        assert_eq!(writes.unverified_legacy_count(), 0);
        assert_eq!(writes.ready_keys(Instant::now() + Duration::from_secs(60), 1).len(), 1);

        let mut restarted = PartialWrites::default();
        restarted
            .admit_with_source_incarnation(original, Some(incarnation), 1, 8192)
            .expect("the unchanged journal re-arms the same responsibility after restart");
        restarted.mark_persisted();
        assert_eq!(restarted.unverified_legacy_count(), 0);
        assert_eq!(restarted.ready_keys(Instant::now() + Duration::from_secs(60), 1).len(), 1);
    }

    #[test]
    fn lifecycle_checkpoint_restores_hold_and_risk_acceptance_without_claiming_proof() {
        let original = intent("legacy-lifecycle");
        let incarnation = Uuid::new_v4();
        let mut writes = PartialWrites::default();
        writes
            .admit_with_source_incarnation(original.clone(), Some(incarnation), 1, 8192)
            .expect("retain durable intent");
        writes.mark_persisted();
        let responsibility_id = writes
            .entries
            .get(&PartialWriteKey::new(&original, Some(incarnation)))
            .expect("resident responsibility")
            .responsibility_id;
        let anchor = MrfDurableRepairAnchor::from_intent(&original, incarnation).expect("exact durable anchor");
        writes
            .entries
            .get_mut(&PartialWriteKey::new(&original, Some(incarnation)))
            .expect("resident responsibility")
            .anchor = Some(anchor.clone());
        assert!(writes.park_unverified_legacy(&anchor));

        let checkpoint = writes
            .checkpoint_records(|_| Some([7; 32]))
            .into_iter()
            .next()
            .expect("hold state checkpoint");
        let mut restored = PartialWrites::default();
        restored
            .admit_with_source_incarnation(original, Some(incarnation), 1, 8192)
            .expect("replay journal intent before applying lifecycle state");
        assert!(restored.restore_state(&intent("legacy-lifecycle"), &checkpoint));
        restored.mark_persisted();
        assert_eq!(restored.unverified_legacy_count(), 1);
        assert!(restored.ready_keys(Instant::now() + Duration::from_secs(60), 1).is_empty());

        let (previous, _) = restored
            .record_operator_acceptance(
                8192,
                risk_acceptance(
                    responsibility_id,
                    incarnation,
                    true,
                    false,
                    "Reviewed against trusted backup; accepting unresolved identity risk",
                    "INC-1234",
                ),
            )
            .expect("explicit risk acceptance");
        assert!(matches!(previous, ResponsibilityState::HeldUnverifiedLegacy { .. }));
        assert_eq!(restored.unverified_legacy_count(), 0);
        assert_eq!(restored.operator_accepted_unverified_count(), 1);
        let accepted_entry = restored
            .entries
            .get(&PartialWriteKey::new(&intent("legacy-lifecycle"), Some(incarnation)))
            .expect("accepted entry");
        assert_eq!(
            restored.bytes(),
            PartialWrites::cost_with_state_and_audit(
                &accepted_entry.intent,
                &accepted_entry.state,
                accepted_entry.last_operator_acceptance.as_ref(),
            )
        );
        let (mut items, next_cursor) = restored.list_unverified_legacy(None, 10);
        let item = items.pop().expect("operator state remains visible");
        assert!(next_cursor.is_none());
        assert_eq!(item.status, "operator_accepted_unverified");

        let (_, accepted) = restored
            .recheck(responsibility_id, incarnation)
            .expect("explicit targeted recheck");
        assert!(matches!(accepted, ResponsibilityState::OperatorAcceptedUnverified { .. }));
        assert_eq!(restored.operator_accepted_unverified_count(), 0);
        let active_entry = restored
            .entries
            .get(&PartialWriteKey::new(&intent("legacy-lifecycle"), Some(incarnation)))
            .expect("active entry");
        assert_eq!(
            restored.bytes(),
            PartialWrites::cost_with_state_and_audit(
                &active_entry.intent,
                &active_entry.state,
                active_entry.last_operator_acceptance.as_ref(),
            ),
            "recheck must continue accounting for the retained audit record"
        );
        restored.mark_persisted();
        assert_eq!(restored.ready_keys(Instant::now() + Duration::from_secs(60), 1).len(), 1);
    }

    #[test]
    fn restored_held_entry_can_be_refreshed_and_retry_index_cleanup_is_linear() {
        let original = intent("refresh-after-recreate");
        let original_incarnation = Uuid::new_v4();
        let recreated_incarnation = Uuid::new_v4();
        let mut writes = PartialWrites::default();
        writes
            .admit_with_source_incarnation(original.clone(), Some(original_incarnation), 1, 8192)
            .expect("durable intent should be retained");
        writes.mark_persisted();
        let anchor = MrfDurableRepairAnchor::from_intent(&original, original_incarnation).expect("anchor");
        writes
            .entries
            .get_mut(&PartialWriteKey::new(&original, Some(original_incarnation)))
            .expect("entry")
            .anchor = Some(anchor.clone());
        assert!(writes.park_unverified_legacy(&anchor));
        let checkpoint = writes.checkpoint_records(|_| Some([4; 32])).remove(0);

        let mut restored = PartialWrites::default();
        restored
            .admit_with_source_incarnation(original.clone(), Some(original_incarnation), 1, 8192)
            .expect("replay creates retry index entry");
        assert!(restored.restore_state(&original, &checkpoint));
        let (previous, _) = restored
            .replace_observed_incarnation(
                checkpoint.responsibility_id,
                original_incarnation,
                recreated_incarnation,
                Some(original_incarnation),
            )
            .expect("refresh should reclassify the changed generation");
        assert!(matches!(
            restored.intent_for_responsibility(checkpoint.responsibility_id),
            Some((_, ResponsibilityState::BucketIncarnationChanged { observed_bucket_incarnation_id, .. }, _))
                if observed_bucket_incarnation_id == recreated_incarnation
        ));
        assert!(
            restored
                .retry_index
                .contains(&PartialWriteKey::new(&original, Some(original_incarnation)))
        );
        assert_eq!(restored.retry_order.len(), 1, "restoration must not scan/reinsert the full retry queue");
        restored.restore_operator_state(checkpoint.responsibility_id, previous, None);
        assert!(
            restored
                .retry_index
                .contains(&PartialWriteKey::new(&original, Some(original_incarnation)))
        );
        assert_eq!(restored.retry_order.len(), 1, "rollback leaves one lazily cleaned queue key");
    }

    #[test]
    fn lifecycle_actions_reject_stale_generation_and_incarnation() {
        let original = intent("legacy-stale");
        let incarnation = Uuid::new_v4();
        let mut writes = PartialWrites::default();
        writes
            .admit_with_source_incarnation(original.clone(), Some(incarnation), 1, 8192)
            .expect("retain durable intent");
        let anchor = MrfDurableRepairAnchor::from_intent(&original, incarnation).expect("exact durable anchor");
        writes
            .entries
            .get_mut(&PartialWriteKey::new(&original, Some(incarnation)))
            .expect("resident responsibility")
            .anchor = Some(anchor.clone());
        assert!(writes.park_unverified_legacy(&anchor));
        let id = writes
            .entries
            .get(&PartialWriteKey::new(&original, Some(incarnation)))
            .expect("entry")
            .responsibility_id;

        assert!(
            writes
                .record_operator_acceptance(8192, risk_acceptance(id, Uuid::new_v4(), true, false, "reason", "INC-1234"))
                .is_err()
        );
        assert!(
            writes
                .record_operator_acceptance(8192, risk_acceptance(Uuid::new_v4(), incarnation, true, false, "reason", "INC-1234"))
                .is_err()
        );
        assert!(
            writes
                .record_operator_acceptance(8192, risk_acceptance(id, incarnation, true, false, " ", "INC-1234"))
                .is_err()
        );
        assert_eq!(writes.unverified_legacy_count(), 1);
    }

    #[test]
    fn source_bucket_incarnation_mismatch_is_parked_until_explicit_risk_ack() {
        let source = Uuid::new_v4();
        let observed = Uuid::new_v4();
        let item = intent("recreated-bucket");
        let mut writes = PartialWrites::default();
        writes
            .admit_with_source_incarnation(item.clone(), Some(source), 1, 8192)
            .expect("retain generation-bound durable intent");
        let key = PartialWriteKey::new(&item, Some(source));
        let entry = writes.entries.get_mut(&key).expect("resident intent");
        entry.state = ResponsibilityState::BucketIncarnationChanged {
            source_bucket_incarnation_id: source,
            observed_bucket_incarnation_id: observed,
            detected_at_ms: unix_now_ms(),
        };
        let responsibility_id = entry.responsibility_id;

        assert!(
            writes
                .record_operator_acceptance(
                    8192,
                    risk_acceptance(
                        responsibility_id,
                        observed,
                        false,
                        false,
                        "The old bucket generation is no longer available",
                        "INC-5678",
                    ),
                )
                .is_err()
        );
        let (accepted, _) = writes
            .record_operator_acceptance(
                8192,
                risk_acceptance(
                    responsibility_id,
                    observed,
                    false,
                    true,
                    "The old bucket generation is no longer available",
                    "INC-5678",
                ),
            )
            .expect("generation mismatch requires and accepts explicit acknowledgment");
        assert!(matches!(accepted, ResponsibilityState::BucketIncarnationChanged { .. }));
        let (items, _) = writes.list_unverified_legacy(None, 10);
        let item = items.into_iter().next().expect("mismatch disposition remains visible");
        assert_eq!(item.source_bucket_incarnation_id, Some(source));
        assert!(item.accepted.is_some_and(|audit| audit.acknowledged_incarnation_mismatch));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn same_key_bucket_incarnations_keep_separate_lifecycle_records_through_replay() {
        use crate::heal::mrf_queue::{MrfConsumerConfig, MrfQueue, MrfRuntime};

        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .prefix("rustfs_mrf_same_key_incarnation_replay")
            .build()
            .await;
        let bucket = "partial-write-retention";
        env.make_bucket(bucket, false).await;
        let storage: Arc<dyn HealStorageAPI> = Arc::new(ECStoreHealStorage::new(env.ecstore.clone()));
        let manager = Arc::new(HealManager::new_without_root_recovery_for_test(storage, None));
        let disks = super::super::journal_disks().await;
        assert!(!disks.is_empty(), "test environment must register local MRF disks");

        let old_incarnation = Uuid::new_v4();
        let current_incarnation = env.ecstore.pools[0]
            .get_disks(0)
            .bucket_incarnation_id_from_disk(bucket)
            .await
            .expect("current bucket incarnation must be available");
        assert_ne!(old_incarnation, current_incarnation);

        let old_intent = intent("same-object");
        let mut writes = PartialWrites::default();
        writes
            .admit_with_source_incarnation(old_intent.clone(), Some(old_incarnation), 2, 64 * 1024)
            .expect("retain the old bucket's partial-write responsibility");
        let old_key = PartialWriteKey::new(&old_intent, Some(old_incarnation));
        let old_entry = writes.entries.get(&old_key).expect("old responsibility should be resident");
        let old_id = old_entry.responsibility_id;
        let retained_old_intent = old_entry.intent.clone();
        let old_anchor =
            super::super::MrfDurableRepairAnchor::from_intent(&retained_old_intent, old_incarnation).expect("old proof anchor");
        writes
            .entries
            .get_mut(&old_key)
            .expect("old responsibility remains resident")
            .anchor = Some(old_anchor.clone());
        assert!(writes.park_unverified_legacy(&old_anchor));
        let old_request_id = Uuid::new_v4();
        writes
            .record_operator_acceptance(
                64 * 1024,
                MrfLegacyRiskAcceptanceRequest {
                    responsibility_id: old_id,
                    expected_bucket_incarnation_id: old_incarnation,
                    acknowledge_unknown_source_incarnation: false,
                    acknowledge_incarnation_mismatch: false,
                    actor: "integration-test-operator".to_string(),
                    reason: "Keep the old bucket generation's disposition attached to its own responsibility".to_string(),
                    reference: "TEST-ISSUE-8192-G1".to_string(),
                    request_id: old_request_id,
                },
            )
            .expect("old-generation risk disposition should be recorded");

        let current_intent = intent("same-object");
        assert_eq!(
            super::super::intent_digest(&retained_old_intent),
            super::super::intent_digest(&current_intent),
            "the two bucket incarnations must exercise the same journal identity digest"
        );
        writes
            .admit_with_source_incarnation(current_intent.clone(), Some(current_incarnation), 2, 64 * 1024)
            .expect("retain the replacement bucket's separate responsibility");
        writes.mark_persisted();
        assert_eq!(writes.depth(), 2, "different bucket incarnations are distinct responsibilities");
        let current_key = PartialWriteKey::new(&current_intent, Some(current_incarnation));
        let current_entry = writes
            .entries
            .get(&current_key)
            .expect("current responsibility should be resident");
        let current_id = current_entry.responsibility_id;
        assert_ne!(old_id, current_id);
        assert!(matches!(current_entry.state, ResponsibilityState::Active));
        assert!(current_entry.last_operator_acceptance.is_none());
        assert!(
            !writes.park_unverified_legacy(&old_anchor),
            "a delayed G1 hold event must not alter the G2 responsibility"
        );
        assert!(matches!(
            writes.intent_for_responsibility(current_id),
            Some((_, ResponsibilityState::Active, Some(source))) if source == current_incarnation
        ));

        let mut journal = Vec::new();
        for pending in writes.intents() {
            assert!(
                super::super::encode_intent(pending, &mut journal),
                "both generations must fit the journal"
            );
        }
        let (decoded_intents, truncated) = super::super::decode_journal(&journal);
        assert_eq!(truncated, 0);
        assert_eq!(decoded_intents.len(), 2, "the journal must retain both incarnations");
        assert_eq!(
            super::super::intent_digest(&decoded_intents[0]),
            super::super::intent_digest(&decoded_intents[1])
        );

        let owner = Uuid::new_v4();
        let sequence = 7;
        let config = MrfConsumerConfig::default();
        let lifecycle_limit = config.journal_max_bytes.saturating_mul(4);
        let records = writes.checkpoint_records(super::super::intent_digest);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].intent_digest, records[1].intent_digest);
        let lifecycle = super::super::encode_mrf_lifecycle_checkpoint(owner, sequence, records, lifecycle_limit)
            .expect("same-key generations need distinct lifecycle records");
        super::super::snapshot::publish_committed_snapshot_with_companion(
            &disks,
            owner,
            sequence,
            &journal,
            config.journal_max_bytes,
            Some((&super::super::MRF_LIFECYCLE_PATHS, &lifecycle, lifecycle_limit)),
        )
        .await
        .expect("publish journal and both lifecycle records together");

        let mut queue = MrfQueue::new(config.queue_capacity, config.journal_max_bytes);
        let replay = super::super::replay_into(&manager, &mut queue, &mut None).await;
        assert_eq!(replay.replayed, 2);
        assert_eq!(queue.depth(), 0, "durable generations bypass ordinary MRF coalescing");
        assert_eq!(replay.partial_writes.len(), 2, "replay must preserve both responsibilities");

        let mut runtime = MrfRuntime {
            partial_writes: PartialWrites::default(),
            queue,
            config,
            checkpoint_owner: Uuid::new_v4(),
            next_checkpoint_sequence: replay.next_checkpoint_sequence,
            new_since_flush: 0,
            dirty: false,
            journal_on_disk: replay.journal_on_disk,
            retain_replay_journal: replay.retain_journal_for_replay,
            durable_replay_anchors: replay.durable_replay_anchors,
            replay_cleanup: replay.cleanup,
            runtime_checkpoint: None,
            backoff_until: None,
        };
        runtime.adopt_replayed_partial_writes(replay.partial_writes);
        assert_eq!(runtime.partial_writes.depth(), 2);
        assert!(matches!(
            runtime.partial_writes.intent_for_responsibility(old_id),
            Some((_, ResponsibilityState::OperatorAcceptedUnverified { request_id, .. }, Some(source)))
                if source == old_incarnation && request_id == old_request_id
        ));
        assert!(matches!(
            runtime.partial_writes.intent_for_responsibility(current_id),
            Some((_, ResponsibilityState::Active, Some(source))) if source == current_incarnation
        ));
        assert!(runtime.partial_writes.last_operator_acceptance(current_id).is_none());

        let replayed_old_intent = runtime
            .partial_writes
            .intent_for_responsibility(old_id)
            .expect("old lifecycle responsibility should remain addressable")
            .0;
        let replayed_current_intent = runtime
            .partial_writes
            .intent_for_responsibility(current_id)
            .expect("current lifecycle responsibility should remain addressable")
            .0;
        let replayed_old_anchor = super::super::MrfDurableRepairAnchor::from_intent(&replayed_old_intent, old_incarnation)
            .expect("replayed old-generation proof anchor");
        let replayed_current_anchor =
            super::super::MrfDurableRepairAnchor::from_intent(&replayed_current_intent, current_incarnation)
                .expect("replayed current-generation proof anchor");
        runtime
            .partial_writes
            .entries
            .get_mut(&PartialWriteKey::new(&replayed_old_intent, Some(old_incarnation)))
            .expect("old-generation entry after replay")
            .anchor = Some(replayed_old_anchor.clone());
        runtime
            .partial_writes
            .entries
            .get_mut(&PartialWriteKey::new(&replayed_current_intent, Some(current_incarnation)))
            .expect("current-generation entry after replay")
            .anchor = Some(replayed_current_anchor.clone());

        let verified_event = |anchor: &super::super::MrfDurableRepairAnchor| MrfVerifiedRepairEvent {
            kind: anchor.kind,
            bucket: anchor.bucket.clone(),
            object: anchor.object.clone(),
            version_id: anchor.version_id,
            scope: anchor.scope,
            delete_marker_purge: anchor.delete_marker_purge,
            lease: Some(anchor.lease),
            bucket_incarnation_id: anchor.bucket_incarnation_id,
            disposition: MrfVerifiedRepairDisposition::Repaired,
        };
        rustfs_common::mrf_channel::note_mrf_verified_repair(verified_event(&replayed_old_anchor));
        runtime.discharge_durable_replay_anchors();
        assert!(runtime.partial_writes.intent_for_responsibility(old_id).is_none());
        assert!(
            runtime.partial_writes.intent_for_responsibility(current_id).is_some(),
            "a matching G1 proof must not release G2"
        );
        rustfs_common::mrf_channel::note_mrf_verified_repair(verified_event(&replayed_current_anchor));
        runtime.discharge_durable_replay_anchors();
        assert!(runtime.partial_writes.intent_for_responsibility(current_id).is_none());
    }

    #[test]
    fn lifecycle_listing_uses_stable_bounded_cursors() {
        let mut writes = PartialWrites::default();
        for (index, object) in ["a", "b", "c", "d"].into_iter().enumerate() {
            let item = intent(object);
            let incarnation = Uuid::from_u128(100 + u128::try_from(index).expect("small test index"));
            writes
                .admit_with_source_incarnation(item.clone(), Some(incarnation), 4, 8192)
                .expect("retain durable intent");
            let key = PartialWriteKey::new(&item, Some(incarnation));
            let entry = writes.entries.get_mut(&key).expect("resident intent");
            entry.responsibility_id = Uuid::from_u128(u128::try_from(index + 1).expect("small test index"));
            let anchor = MrfDurableRepairAnchor::from_intent(&item, incarnation).expect("exact durable anchor");
            entry.anchor = Some(anchor.clone());
            assert!(writes.park_unverified_legacy(&anchor));
        }

        let (first, cursor) = writes.list_unverified_legacy(None, 2);
        assert_eq!(first.len(), 2);
        let cursor = cursor.expect("more entries should expose a continuation cursor");
        let (second, next_cursor) = writes.list_unverified_legacy(Some(cursor), 2);
        assert_eq!(second.len(), 2);
        assert!(next_cursor.is_none(), "the last page must omit its cursor");
        assert!(first.last().expect("first page").responsibility_id < second[0].responsibility_id);
    }

    #[test]
    fn partial_write_retention_adopts_replay_anchor_before_generation_replacement() {
        use super::super::{MrfQueue, MrfRuntime};
        let old = intent("replayed");
        let anchor = MrfDurableRepairAnchor::from_intent(&old, Uuid::new_v4()).expect("replay anchor");
        let config = MrfConsumerConfig::default();
        let mut runtime = MrfRuntime {
            queue: MrfQueue::new(config.queue_capacity, config.journal_max_bytes),
            partial_writes: PartialWrites::default(),
            config,
            checkpoint_owner: Uuid::new_v4(),
            next_checkpoint_sequence: 1,
            new_since_flush: 0,
            dirty: false,
            journal_on_disk: true,
            retain_replay_journal: false,
            durable_replay_anchors: vec![anchor],
            replay_cleanup: None,
            runtime_checkpoint: None,
            backoff_until: None,
        };
        runtime.adopt_replayed_partial_writes(vec![(old, None)]);
        assert!(
            runtime.durable_replay_anchors.is_empty(),
            "the live record must be the single proof owner"
        );
        assert!(runtime.retained_replay_journal(), "adoption cannot release the startup checkpoint");
        runtime
            .admit_partial_write(intent("replayed"), None)
            .expect("new generation should be retained");
        assert!(
            runtime.retained_replay_journal(),
            "replacement must retain responsibility before its checkpoint"
        );
        assert_eq!(runtime.partial_writes.depth(), 1);
        assert!(runtime.dirty);
    }

    #[test]
    fn partial_write_retention_new_generation_rejects_old_proof_and_requires_checkpoint() {
        let mut writes = PartialWrites::default();
        let first = intent("same-key");
        let key = PartialWriteKey::new(&first, None);
        let incarnation = Uuid::new_v4();
        let old_anchor = MrfDurableRepairAnchor::from_intent(&first, incarnation).expect("first anchor should be complete");
        writes.admit(first, 1, 4096).expect("first write should fit");
        writes.entries.get_mut(&key).expect("first entry should exist").anchor = Some(old_anchor.clone());
        writes.mark_persisted();
        let second = intent("same-key");
        let new_anchor = MrfDurableRepairAnchor::from_intent(&second, incarnation).expect("second anchor should be complete");
        assert_ne!(old_anchor.lease, new_anchor.lease);
        writes
            .admit(second, 1, 4096)
            .expect("replacement should share the bounded slot");
        assert!(!writes.entries[&key].persisted, "a previous checkpoint does not admit the new generation");
        assert!(
            !writes.retain_unproven(&HashSet::new()),
            "an old proof cannot remove a generation without an anchor"
        );
        writes.entries.get_mut(&key).expect("replacement should exist").anchor = Some(new_anchor.clone());
        assert!(!writes.retain_unproven(&HashSet::from([new_anchor])));
        assert_eq!(writes.depth(), 1, "unproven replacement must remain even after exhausting hint attempts");
        assert!(
            writes.retain_unproven(&HashSet::new()),
            "only the replacement's matched proof releases it"
        );
        assert_eq!(writes.bytes(), 0);
    }
}
