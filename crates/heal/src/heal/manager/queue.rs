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
/// The priority heal queue and its per-key dedup index.
use super::*;

/// Per-key bookkeeping for the queued-request dedup index: how many queued
/// requests hold the key, and the id of the first request that opened it —
/// the O(1) stand-in for the former heap scan when a merge receipt needs to
/// name a queued representative.
#[derive(Debug)]
pub(super) struct DedupKeyEntry {
    pub(super) refcount: usize,
    pub(super) representative_request_id: String,
}

/// Priority queue wrapper for heal requests
/// Uses BinaryHeap for priority-based ordering while maintaining FIFO for same-priority items
#[derive(Debug)]
pub(super) struct PriorityHealQueue {
    /// Heap of (priority, sequence, request) tuples
    pub(super) heap: BinaryHeap<PriorityQueueItem>,
    /// Sequence counter for FIFO ordering within same priority
    pub(super) sequence: u64,
    /// Deduplication index for queued requests
    pub(super) dedup_keys: HashMap<String, DedupKeyEntry>,
}

/// Wrapper for heap items to implement proper ordering
#[derive(Debug)]
pub(super) struct PriorityQueueItem {
    pub(super) priority: HealPriority,
    pub(super) sequence: u64,
    pub(super) dedup_key: String,
    pub(super) request: HealRequest,
}

impl Eq for PriorityQueueItem {}

impl PartialEq for PriorityQueueItem {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Ord for PriorityQueueItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // First compare by priority (higher priority first)
        match self.priority.cmp(&other.priority) {
            std::cmp::Ordering::Equal => {
                // If priorities are equal, use sequence for FIFO (lower sequence first)
                other.sequence.cmp(&self.sequence)
            }
            ordering => ordering,
        }
    }
}

impl PartialOrd for PriorityQueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum QueuePushOutcome {
    Accepted,
    Merged,
}

#[derive(Debug, Clone)]
pub(super) struct CompletedHealStatus {
    pub(super) heal_type: HealType,
    pub(super) status: HealTaskStatus,
    pub(super) progress: Option<HealProgress>,
    pub(super) outcome: Option<Arc<HealTaskOutcome>>,
    pub(super) retained_bytes: std::sync::OnceLock<usize>,
    pub(super) result_items_truncated: bool,
    pub(super) completed_at: SystemTime,
    /// Sequence-stamped retained window, archived with the completion so
    /// incremental consumers keep their cursor across the transition (HS-06).
    /// The un-stamped legacy view is derived from it on demand.
    pub(super) seqed_items: Vec<(u64, HealResultItem)>,
    pub(super) next_seq: u64,
    pub(super) min_seq: u64,
}

impl CompletedHealStatus {
    // Account for owned capacities, including nested drive arrays. Aliases
    // conservatively charge the shared allocation again, keeping both token
    // count and retained payload bounded without a second ownership index.
    pub(super) fn retained_bytes(&self) -> usize {
        *self.retained_bytes.get_or_init(|| self.measure_retained_bytes())
    }

    fn measure_retained_bytes(&self) -> usize {
        let mut bytes = size_of::<Self>();
        let mut add = |amount: usize| bytes = bytes.saturating_add(amount);
        add(self.outcome.as_ref().map_or(0, |outcome| outcome.retained_bytes()));
        match &self.heal_type {
            HealType::Cluster => {}
            HealType::Bucket { bucket } => add(bucket.capacity()),
            HealType::Object {
                bucket,
                object,
                version_id,
            }
            | HealType::ECDecode {
                bucket,
                object,
                version_id,
            } => {
                add(bucket.capacity());
                add(object.capacity());
                add(version_id.as_ref().map_or(0, String::capacity));
            }
            HealType::Prefix { bucket, prefix } => {
                add(bucket.capacity());
                add(prefix.capacity());
            }
            HealType::Metadata { bucket, object } => {
                add(bucket.capacity());
                add(object.capacity());
            }
            HealType::ErasureSet { buckets, set_disk_id } => {
                add(buckets.capacity().saturating_mul(size_of::<String>()));
                for bucket in buckets {
                    add(bucket.capacity());
                }
                add(set_disk_id.capacity());
            }
        }
        if let HealTaskStatus::Failed { error } | HealTaskStatus::Retrying { error, .. } = &self.status {
            add(error.capacity());
        }
        add(self
            .progress
            .as_ref()
            .and_then(|progress| progress.current_object.as_ref())
            .map_or(0, String::capacity));
        add(self.seqed_items.capacity().saturating_mul(size_of::<(u64, HealResultItem)>()));
        for (_, item) in &self.seqed_items {
            add(Self::result_item_heap_bytes(item));
        }
        bytes
    }

    fn result_item_heap_bytes(item: &HealResultItem) -> usize {
        let mut bytes = 0usize;
        let mut add = |amount: usize| bytes = bytes.saturating_add(amount);
        for value in [
            &item.heal_item_type,
            &item.bucket,
            &item.object,
            &item.version_id,
            &item.detail,
        ] {
            add(value.capacity());
        }
        for infos in [&item.before, &item.after] {
            add(infos
                .drives
                .capacity()
                .saturating_mul(size_of::<rustfs_madmin::heal_commands::HealDriveInfo>()));
            for drive in &infos.drives {
                add(drive.uuid.capacity());
                add(drive.endpoint.capacity());
                add(drive.state.capacity());
            }
        }
        bytes
    }

    pub(super) fn bound_result_window(&mut self) {
        let mut bytes = 0usize;
        let retained = self
            .seqed_items
            .iter()
            .rev()
            .take_while(|(_, item)| {
                bytes = bytes
                    .saturating_add(size_of::<(u64, HealResultItem)>())
                    .saturating_add(Self::result_item_heap_bytes(item));
                bytes <= MAX_COMPLETED_HEAL_RESULT_BYTES
            })
            .count();
        let truncated = retained < self.seqed_items.len();
        if truncated {
            self.seqed_items.drain(..self.seqed_items.len() - retained);
            self.seqed_items.shrink_to_fit();
            self.min_seq = self.seqed_items.first().map_or(self.next_seq, |(seq, _)| *seq);
            self.result_items_truncated = true;
            self.retained_bytes.take();
        }
    }

    pub(super) async fn snapshot(task: &HealTask, status: HealTaskStatus) -> Self {
        let seqed_items = task.get_seqed_result_items().await;
        let (next_seq, min_seq) = task.result_seq_cursors();
        let mut snapshot = Self {
            heal_type: task.heal_type.clone(),
            status,
            progress: Some(task.get_progress().await),
            outcome: Some(Arc::new(task.get_outcome().await)),
            retained_bytes: std::sync::OnceLock::new(),
            result_items_truncated: task.result_items_truncated(),
            completed_at: SystemTime::now(),
            seqed_items,
            next_seq,
            min_seq,
        };
        snapshot.bound_result_window();
        snapshot
    }
}

#[derive(Debug, Clone)]
pub(super) struct HealTaskAlias {
    pub(super) task_id: String,
}

#[derive(Debug, Clone)]
pub(super) struct RetryingHeal {
    pub(super) request: HealRequest,
    pub(super) error: String,
    pub(super) cancel_token: CancellationToken,
}

impl PriorityHealQueue {
    pub(super) fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            sequence: 0,
            dedup_keys: HashMap::new(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.heap.len()
    }

    pub(super) fn pop_next(&mut self) -> Option<HealRequest> {
        self.heap.pop().map(|item| {
            Self::decrement_or_remove_dedup_key(&mut self.dedup_keys, &item.dedup_key);
            item.request
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub(super) fn push(&mut self, request: HealRequest) -> QueuePushOutcome {
        let key = Self::make_dedup_key(&request);

        // Check for duplicates unless the caller explicitly forces admission.
        if self.dedup_keys.contains_key(&key) && !request.force_start {
            return QueuePushOutcome::Merged;
        }
        // Track dedup keys for both normal and forced requests so queued forced work
        // also reserves the dedup key for later non-forced duplicates. The first
        // request that opens the key becomes the named representative for merge
        // receipts (taken before `request` moves into the heap).
        self.dedup_keys
            .entry(key.clone())
            .or_insert_with(|| DedupKeyEntry {
                refcount: 0,
                representative_request_id: request.id.clone(),
            })
            .refcount += 1;
        self.sequence += 1;
        self.heap.push(PriorityQueueItem {
            priority: request.priority,
            sequence: self.sequence,
            dedup_key: key,
            request,
        });
        QueuePushOutcome::Accepted
    }

    pub(super) fn can_displace_lower_priority(&self, priority: HealPriority) -> bool {
        self.heap.iter().any(|item| item.priority < priority)
    }

    pub(super) fn push_displacing_lower_priority(&mut self, request: HealRequest) -> Option<HealRequest> {
        let mut retained = BinaryHeap::new();
        let mut displaced: Option<PriorityQueueItem> = None;

        while let Some(item) = self.heap.pop() {
            if item.priority < request.priority {
                let should_displace = displaced
                    .as_ref()
                    .map(|current| {
                        item.priority < current.priority
                            || (item.priority == current.priority && item.sequence > current.sequence)
                    })
                    .unwrap_or(true);
                if should_displace {
                    if let Some(current) = displaced.replace(item) {
                        retained.push(current);
                    }
                } else {
                    retained.push(item);
                }
            } else {
                retained.push(item);
            }
        }

        self.heap = retained;

        let displaced = displaced.map(|item| {
            Self::decrement_or_remove_dedup_key(&mut self.dedup_keys, &item.dedup_key);
            self.refresh_dedup_representative(&item.dedup_key);
            item.request
        });

        if displaced.is_some() {
            // The enqueue side effect must run in ALL builds. Do NOT fold `self.push(request)`
            // into `debug_assert_eq!` — in release builds (`debug_assertions` off) the whole
            // macro, including its argument expression, is compiled out, which would silently
            // drop the new high-priority request after having already evicted a queued item.
            let outcome = self.push(request);
            debug_assert_eq!(outcome, QueuePushOutcome::Accepted);
        }

        displaced
    }

    /// Get statistics about queue contents by priority
    pub(super) fn get_priority_stats(&self) -> HashMap<HealPriority, usize> {
        let mut stats = HashMap::new();
        for item in &self.heap {
            *stats.entry(item.priority).or_insert(0) += 1;
        }
        stats
    }

    pub(super) fn operation_counts(&self) -> (HealPriorityCounts, HealSourceCounts) {
        let mut priority = HealPriorityCounts::default();
        let mut source = HealSourceCounts::default();
        for item in &self.heap {
            priority.increment(item.request.priority);
            source.increment(item.request.source);
        }
        (priority, source)
    }

    #[cfg(test)]
    pub(super) fn pop(&mut self) -> Option<HealRequest> {
        self.heap.pop().map(|item| {
            Self::decrement_or_remove_dedup_key(&mut self.dedup_keys, &item.dedup_key);
            item.request
        })
    }

    pub(super) fn pop_runnable_with_skips<F, G>(&mut self, can_run: F, skip_label: G) -> (Option<HealRequest>, Vec<String>)
    where
        F: Fn(&HealRequest) -> bool,
        G: Fn(&HealRequest) -> Option<String>,
    {
        let mut deferred = Vec::new();
        let mut selected = None;
        let mut skipped = Vec::new();

        while let Some(item) = self.heap.pop() {
            if can_run(&item.request) {
                selected = Some(item);
                break;
            }
            if let Some(label) = skip_label(&item.request) {
                skipped.push(label);
            }
            deferred.push(item);
        }

        self.restore_deferred_items(deferred);

        (
            selected.map(|item| {
                Self::decrement_or_remove_dedup_key(&mut self.dedup_keys, &item.dedup_key);
                item.request
            }),
            skipped,
        )
    }

    fn restore_deferred_items(&mut self, deferred: Vec<PriorityQueueItem>) {
        if deferred.is_empty() {
            return;
        }

        if deferred.len() > self.heap.len() / 2 {
            let mut items = std::mem::take(&mut self.heap).into_vec();
            items.reserve(deferred.len());
            items.extend(deferred);
            self.heap = BinaryHeap::from(items);
        } else {
            for item in deferred {
                self.heap.push(item);
            }
        }
    }

    /// Create a deduplication key from a heal request
    pub(super) fn make_dedup_key(request: &HealRequest) -> String {
        let base = Self::make_dedup_key_for_type(&request.heal_type);
        match (&request.heal_type, request.options.set_key()) {
            (HealType::Object { .. } | HealType::ECDecode { .. }, Some(scope)) => format!("{base}:scope:{scope}"),
            _ => base,
        }
    }

    pub(super) fn make_dedup_key_for_type(heal_type: &HealType) -> String {
        match heal_type {
            HealType::Cluster => "cluster".to_string(),
            HealType::Object {
                bucket,
                object,
                version_id,
            } => {
                format!("object:{}:{}:{}", bucket, object, version_id.as_deref().unwrap_or(""))
            }
            HealType::Bucket { bucket } => {
                format!("bucket:{bucket}")
            }
            HealType::Prefix { bucket, prefix } => {
                format!("prefix:{bucket}/{prefix}")
            }
            HealType::ErasureSet { set_disk_id, .. } => {
                format!("erasure_set:{set_disk_id}")
            }
            HealType::Metadata { bucket, object } => {
                format!("metadata:{bucket}:{object}")
            }
            HealType::ECDecode {
                bucket,
                object,
                version_id,
            } => {
                format!("ecdecode:{}:{}:{}", bucket, object, version_id.as_deref().unwrap_or(""))
            }
        }
    }

    pub(super) fn decrement_or_remove_dedup_key(dedup_keys: &mut HashMap<String, DedupKeyEntry>, key: &str) {
        if let Some(entry) = dedup_keys.get_mut(key) {
            if entry.refcount <= 1 {
                dedup_keys.remove(key);
            } else {
                entry.refcount -= 1;
            }
        }
    }
    /// Check if an erasure set heal request for a specific set_disk_id exists
    pub(super) fn contains_erasure_set(&self, set_disk_id: &str) -> bool {
        let key = format!("erasure_set:{set_disk_id}");
        self.dedup_keys.contains_key(&key)
    }

    /// Iterate queued requests (used by the admin overlap check).
    pub(super) fn requests(&self) -> impl Iterator<Item = &HealRequest> {
        self.heap.iter().map(|item| &item.request)
    }

    pub(super) fn contains_request_id(&self, request_id: &str) -> bool {
        self.heap.iter().any(|item| item.request.id == request_id)
    }

    pub(super) fn contains_request_id_matching_path(&self, request_id: &str, heal_path: &str) -> bool {
        self.heap
            .iter()
            .any(|item| item.request.id == request_id && heal_type_matches_path(&item.request.heal_type, heal_path))
    }

    pub(super) fn queued_request_id_for_dedup_key(&self, key: &str) -> Option<&str> {
        self.dedup_keys.get(key).map(|entry| entry.representative_request_id.as_str())
    }

    /// Re-elect the representative for `key` from the queue entries holding
    /// it. Needed after a holder leaves the queue *without* becoming active
    /// (canceled by id, or displaced): the former opener may be the request
    /// that just left, and a merge receipt must never name an id that
    /// resolves nowhere. The scheduler pop path does not need this — the
    /// popped request surfaces in `active_heals` under the same id and the
    /// duplicate pre-check consults active heals before the queue. No-op for
    /// released keys; the survivor scan only runs when a key still has
    /// holders, which under forced duplicates is the rare admin path.
    pub(super) fn refresh_dedup_representative(&mut self, key: &str) {
        if !self.dedup_keys.contains_key(key) {
            return;
        }
        if let Some(id) = self
            .heap
            .iter()
            .find(|item| item.dedup_key == key)
            .map(|item| item.request.id.clone())
            && let Some(entry) = self.dedup_keys.get_mut(key)
        {
            entry.representative_request_id = id;
        }
    }

    pub(super) fn contains_matching<F>(&self, mut matches: F) -> bool
    where
        F: FnMut(&HealRequest) -> bool,
    {
        self.heap.iter().any(|item| matches(&item.request))
    }

    pub(super) fn remove_request_id(&mut self, request_id: &str) -> Option<HealRequest> {
        let mut retained = BinaryHeap::new();
        let mut removed = None;
        let mut affected_key = None;

        while let Some(item) = self.heap.pop() {
            if removed.is_none() && item.request.id == request_id {
                let key = item.dedup_key.clone();
                Self::decrement_or_remove_dedup_key(&mut self.dedup_keys, &key);
                affected_key = Some(key);
                removed = Some(item.request);
            } else {
                retained.push(item);
            }
        }

        self.heap = retained;
        if let Some(key) = affected_key.as_deref() {
            self.refresh_dedup_representative(key);
        }
        removed
    }

    pub(super) fn remove_matching<F>(&mut self, mut should_remove: F) -> Vec<HealRequest>
    where
        F: FnMut(&HealRequest) -> bool,
    {
        let mut retained = BinaryHeap::new();
        let mut removed = Vec::new();
        let mut affected_keys = Vec::new();

        while let Some(item) = self.heap.pop() {
            if should_remove(&item.request) {
                Self::decrement_or_remove_dedup_key(&mut self.dedup_keys, &item.dedup_key);
                affected_keys.push(item.dedup_key);
                removed.push(item.request);
            } else {
                retained.push(item);
            }
        }

        self.heap = retained;
        for key in &affected_keys {
            self.refresh_dedup_representative(key);
        }
        removed
    }
}

impl RetryingHeal {
    pub(super) fn status(&self) -> HealTaskStatus {
        HealTaskStatus::Retrying {
            error: self.error.clone(),
            retry_attempt: self.request.retry_attempts,
        }
    }
}
