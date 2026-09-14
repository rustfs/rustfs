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

use super::{HealManager, MrfConsumerConfig, MrfDurableRepairAnchor, MrfIntent, MrfQueueKey, queue_key, submit_mrf_heal_request};
use rustfs_common::mrf_channel::{MrfDurableAdmissionError, MrfIngressResult, release_mrf_intent, try_rearm_mrf_replay_intent};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::time::Instant;

struct Responsibility {
    intent: MrfIntent,
    anchor: Option<MrfDurableRepairAnchor>,
    persisted: bool,
    next_attempt: Instant,
}

/// Partial writes have no rediscovery producer when scanning is disabled.
/// Admission, task failure and retry exhaustion cannot release their records.
#[derive(Default)]
pub(super) struct PartialWrites {
    entries: HashMap<MrfQueueKey, Responsibility>,
    retry_order: VecDeque<MrfQueueKey>,
    bytes: usize,
}

impl PartialWrites {
    pub(super) fn cost(intent: &MrfIntent) -> usize {
        intent.estimated_bytes() + std::mem::size_of::<Responsibility>() + 2 * std::mem::size_of::<MrfQueueKey>()
    }

    pub(super) fn admit(
        &mut self,
        mut intent: MrfIntent,
        capacity: usize,
        byte_budget: usize,
    ) -> Result<(), MrfDurableAdmissionError> {
        if try_rearm_mrf_replay_intent(&mut intent) != MrfIngressResult::Enqueued {
            return Err(MrfDurableAdmissionError::InvalidIdentity);
        }
        let key = queue_key(&intent);
        let previous = self.entries.get(&key);
        let old_cost = previous.map_or(0, |entry| Self::cost(&entry.intent));
        let next_bytes = self.bytes.saturating_sub(old_cost).saturating_add(Self::cost(&intent));
        if (previous.is_none() && self.entries.len() >= capacity) || next_bytes > byte_budget {
            return Err(MrfDurableAdmissionError::Full);
        }
        if previous.is_some_and(|entry| entry.intent.lease == intent.lease) {
            return Ok(());
        }
        if previous.is_none() {
            self.retry_order.push_back(key.clone());
        }
        // Replacing the generation preserves the logical repair obligation,
        // but requires a new checkpoint and proof before it can be released.
        self.entries.insert(
            key,
            Responsibility {
                intent,
                anchor: None,
                persisted: false,
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

    pub(super) fn mark_persisted(&mut self) {
        for entry in self.entries.values_mut() {
            entry.persisted = true;
        }
    }

    pub(super) async fn dispatch(&mut self, manager: &HealManager, config: &MrfConsumerConfig) {
        let now = Instant::now();
        for key in self.ready_keys(now, config.replay_batch) {
            let Some(entry) = self.entries.get_mut(&key) else {
                continue;
            };
            entry.next_attempt = now + config.admission_backoff;
            if entry.anchor.is_none() {
                entry.anchor = manager.durable_mrf_repair_anchor(&entry.intent).await;
            }
            if entry.anchor.is_some() {
                // Every outcome retains responsibility until a verified proof.
                // A full manager or an offline target only postpones another try.
                let _ = submit_mrf_heal_request(manager, &entry.intent).await;
            }
        }
    }

    fn ready_keys(&mut self, now: Instant, limit: usize) -> Vec<MrfQueueKey> {
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
            let due = self
                .entries
                .get(&key)
                .is_some_and(|entry| entry.persisted && entry.next_attempt <= now);
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
        self.bytes = self.entries.values().map(|entry| Self::cost(&entry.intent)).sum();
        self.retry_order.retain(|key| self.entries.contains_key(key));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_common::mrf_channel::{MrfKind, MrfScope};
    use std::sync::Arc;
    use uuid::Uuid;

    fn intent(object: &str) -> MrfIntent {
        let mut intent = MrfIntent {
            bucket: Arc::from("partial-write-retention"),
            object: Arc::from(object),
            version_id: None,
            kind: MrfKind::PartialWrite,
            scope: Some(MrfScope {
                pool_index: 0,
                set_index: 0,
            }),
            lease: None,
            enqueued_at_ms: 1,
            attempts: u8::MAX,
        };
        assert_eq!(try_rearm_mrf_replay_intent(&mut intent), MrfIngressResult::Enqueued);
        intent
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
        let first: Vec<_> = writes.ready_keys(now, 2).into_iter().map(|key| key.object).collect();
        assert_eq!(first, vec![Arc::<str>::from("a"), Arc::<str>::from("b")]);
        let second = writes.ready_keys(now, 2);
        assert_eq!(
            second[0].object.as_ref(),
            "c",
            "the old prefix becoming due again cannot starve the next member"
        );
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
        runtime.adopt_replayed_partial_writes(vec![old]);
        assert!(
            runtime.durable_replay_anchors.is_empty(),
            "the live record must be the single proof owner"
        );
        assert!(runtime.retained_replay_journal(), "adoption cannot release the startup checkpoint");
        runtime
            .admit_partial_write(intent("replayed"))
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
        let key = queue_key(&first);
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
