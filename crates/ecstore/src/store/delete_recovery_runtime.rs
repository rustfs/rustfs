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

//! Bounded scheduling over the existing on-disk delete journals and receipts.
//! Hints are expendable: startup, returning drives and overflow retain a raw
//! discovery obligation. Only the canonical recovery path decides mutations.

use super::ECStore;
use crate::bucket::utils::is_meta_bucketname;
use crate::disk::{DeleteRecoveryCursor, DiskAPI, DiskStore};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use rustfs_heal_contracts::heal_channel::HealOpts;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const HINT_CAPACITY: usize = 1_024;
const DISCOVERY_PAGE_SIZE: u32 = 128;
const PAGES_PER_PASS: usize = 8;
const MAX_ACTIVE_DISCOVERY_CURSORS: usize = 32;
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_DELETE_RECOVERY: &str = "delete_recovery";
const EVENT_DELETE_RECOVERY_STATUS: &str = "delete_recovery_status";
const KEYS_PER_PASS: usize = 16;
const RECOVERY_CONCURRENCY: usize = 4;
const INTERVAL: Duration = Duration::from_secs(1);
const INVENTORY_INTERVAL: Duration = Duration::from_secs(30);
const DISCOVERY_BUDGET: Duration = Duration::from_secs(5);
const ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(60 * 60);
const PASS_START_BUDGET: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DeleteRecoveryKey {
    pub(crate) pool: usize,
    pub(crate) set: usize,
    pub(crate) bucket: String,
    pub(crate) object: String,
    pub(crate) incarnation: Option<Uuid>,
}

#[derive(Clone, Debug)]
pub(crate) struct DeleteRecoveryTicket {
    key: Arc<DeleteRecoveryKey>,
    generation: Uuid,
    admission: Option<Arc<AdmissionGuard>>,
}

#[derive(Debug)]
struct AdmissionGuard {
    state: Arc<Mutex<HintState>>,
    key: Arc<DeleteRecoveryKey>,
    settled: AtomicBool,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        if !self.settled.load(Ordering::Acquire) {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if !state.pending.contains_key(&self.key) {
                // A scan can finish before an evicted request persists its
                // journal. Re-arm after that request finishes or is canceled.
                state.rescan = Some(Uuid::new_v4());
            }
        }
    }
}

#[derive(Debug)]
struct HintState {
    pending: HashMap<Arc<DeleteRecoveryKey>, (Uuid, Uuid, bool)>,
    order: VecDeque<DeleteRecoveryTicket>,
    rescan: Option<Uuid>,
    started: HashSet<Uuid>,
    #[cfg(test)]
    compaction_visits: usize,
}

impl HintState {
    fn compact_order(&mut self, capacity: usize) {
        // Twice the pending capacity guarantees at least capacity completed
        // entries between compactions, even with a full retry backlog. This
        // amortizes FIFO maintenance across successful delete requests.
        // The queue generation preserves FIFO position across coalescing.
        if self.order.len() < capacity.saturating_mul(2) {
            return;
        }
        #[cfg(test)]
        {
            self.compaction_visits += self.order.len();
        }
        let Self { pending, order, .. } = self;
        order.retain(|ticket| pending.get(&ticket.key).is_some_and(|entry| entry.1 == ticket.generation));
    }
}

pub(crate) struct DeleteRecoveryHints {
    state: Arc<Mutex<HintState>>,
    capacity: usize,
}

impl Default for DeleteRecoveryHints {
    fn default() -> Self {
        Self::with_capacity(HINT_CAPACITY)
    }
}

impl DeleteRecoveryHints {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(HintState {
                pending: HashMap::new(),
                order: VecDeque::new(),
                rescan: Some(Uuid::new_v4()),
                started: HashSet::new(),
                #[cfg(test)]
                compaction_visits: 0,
            })),
            capacity,
        }
    }

    // The queue mutex is held only for in-memory operations and is always
    // released before disk, namespace, bucket lifecycle or movement locks.
    pub(crate) fn register(&self, key: DeleteRecoveryKey) -> Option<DeleteRecoveryTicket> {
        let key = Arc::new(key);
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let generation = Uuid::new_v4();
        if let Some((current, _, _)) = state.pending.get_mut(&key) {
            *current = generation;
        } else {
            if state.pending.len() >= self.capacity {
                // Durable evidence remains the authority. Rotate the oldest
                // hint so permanently blocked keys cannot exclude later raw
                // discoveries forever; the next scan revisits evicted keys.
                while let Some(oldest) = state.order.pop_front() {
                    if state
                        .pending
                        .get(&oldest.key)
                        .is_some_and(|entry| entry.1 == oldest.generation)
                    {
                        state.pending.remove(&oldest.key);
                        break;
                    }
                }
                state.rescan = Some(Uuid::new_v4());
            }
            state.compact_order(self.capacity);
            state.pending.insert(key.clone(), (generation, generation, false));
            state.order.push_back(DeleteRecoveryTicket {
                key: key.clone(),
                generation,
                admission: None,
            });
        }
        Some(DeleteRecoveryTicket {
            admission: Some(Arc::new(AdmissionGuard {
                state: self.state.clone(),
                key: key.clone(),
                settled: AtomicBool::new(false),
            })),
            key,
            generation,
        })
    }

    fn register_discovered(&self, key: DeleteRecoveryKey) -> bool {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.pending.contains_key(&key) {
            return true;
        }
        if state.pending.len() >= self.capacity {
            // Only previously attempted work may yield its slot. Unattempted
            // keys apply backpressure so a fast scan cannot overwrite them.
            let retried = state.order.iter().position(|ticket| {
                state
                    .pending
                    .get(&ticket.key)
                    .is_some_and(|entry| entry.1 == ticket.generation && entry.2)
            });
            let Some(index) = retried else { return false };
            if let Some(oldest) = state.order.remove(index) {
                state.pending.remove(&oldest.key);
            }
            // This durable key has already been attempted. The hourly sweep
            // revisits it; immediate rescans would loop on permanent failures.
        }
        state.compact_order(self.capacity);
        let key = Arc::new(key);
        let generation = Uuid::new_v4();
        state.pending.insert(key.clone(), (generation, generation, false));
        state.order.push_back(DeleteRecoveryTicket {
            key,
            generation,
            admission: None,
        });
        true
    }

    pub(crate) fn ack(&self, ticket: &DeleteRecoveryTicket) {
        if let Some(admission) = &ticket.admission {
            admission.settled.store(true, Ordering::Release);
        }
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .pending
            .get(&ticket.key)
            .is_some_and(|entry| entry.0 == ticket.generation)
        {
            state.pending.remove(&ticket.key);
        }
    }

    fn take_available(&self, active: &HashSet<Arc<DeleteRecoveryKey>>) -> Option<DeleteRecoveryTicket> {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let queued = state.order.len();
        for _ in 0..queued {
            let ticket = state.order.pop_front()?;
            if let Some(&(generation, slot, _)) = state.pending.get(&ticket.key)
                && slot == ticket.generation
            {
                if active.contains(&ticket.key) {
                    // Preserve a newer hint while this key still owns a slot.
                    // Other keys can proceed without waiting on its lock.
                    state.order.push_back(ticket);
                    continue;
                }
                state.pending.remove(&ticket.key);
                return Some(DeleteRecoveryTicket {
                    key: ticket.key,
                    generation,
                    admission: None,
                });
            }
        }
        None
    }

    #[cfg(test)]
    fn take(&self, limit: usize) -> Vec<DeleteRecoveryTicket> {
        let active = HashSet::new();
        std::iter::from_fn(|| self.take_available(&active)).take(limit).collect()
    }

    fn retry(&self, ticket: DeleteRecoveryTicket) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.pending.contains_key(&ticket.key) {
            return;
        }
        state.compact_order(self.capacity);
        if state.pending.len() >= self.capacity {
            // Previously attempted work yields to admitted keys until the
            // periodic sweep. Foreground admission retains its own rescan guard.
            return;
        }
        state
            .pending
            .insert(ticket.key.clone(), (ticket.generation, ticket.generation, true));
        state.order.push_back(ticket);
    }

    #[cfg(test)]
    pub(crate) fn is_pending(&self, key: &DeleteRecoveryKey) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .contains_key(key)
    }

    fn pending_len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .len()
    }

    fn request_rescan(&self) {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).rescan = Some(Uuid::new_v4());
    }

    fn rescan(&self) -> Option<Uuid> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).rescan
    }

    fn ack_rescan(&self, generation: Uuid) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.rescan == Some(generation) {
            state.rescan = None;
        }
    }

    fn start(&self, store: Uuid) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .started
            .insert(store)
    }
}

#[derive(Clone)]
struct LocalSource {
    pool: usize,
    set: usize,
    disk: DiskStore,
}

struct BucketScan {
    bucket: String,
    incarnation: Option<Uuid>,
    cursor: Option<DeleteRecoveryCursor>,
    pending: VecDeque<String>,
}

struct DiskScan {
    source: LocalSource,
    buckets: Option<VecDeque<BucketScan>>,
}

fn admit_discovered_keys(hints: &DeleteRecoveryHints, source: &LocalSource, bucket: &mut BucketScan) {
    while let Some(object) = bucket.pending.front() {
        if !hints.register_discovered(DeleteRecoveryKey {
            pool: source.pool,
            set: source.set,
            bucket: bucket.bucket.clone(),
            object: object.clone(),
            incarnation: bucket.incarnation,
        }) {
            break;
        }
        bucket.pending.pop_front();
    }
}

#[derive(Default)]
struct RecoveryPass {
    sources: Vec<LocalSource>,
    next_inventory: Option<Instant>,
    next_sweep: Option<Instant>,
    scans: VecDeque<DiskScan>,
    scan_generation: Option<Uuid>,
    deferred: VecDeque<(Instant, DiskScan)>,
}

impl RecoveryPass {
    async fn refresh_inventory(&mut self, store: &Arc<ECStore>) {
        if self.next_inventory.is_some_and(|next| Instant::now() < next) {
            return;
        }
        let mut sources = Vec::new();
        for pool in &store.pools {
            for set in &pool.disk_set {
                for disk in set
                    .disk_inventory()
                    .await
                    .into_iter()
                    .flatten()
                    .filter(|disk| disk.is_local())
                {
                    sources.push(LocalSource {
                        pool: set.pool_index,
                        set: set.set_index,
                        disk,
                    });
                }
            }
        }
        let current_source = |source: &LocalSource| {
            sources.iter().any(|current| {
                current.pool == source.pool && current.set == source.set && Arc::ptr_eq(&current.disk, &source.disk)
            })
        };
        if sources.len() != self.sources.len() || self.sources.iter().any(|source| !current_source(source)) {
            store.ctx.delete_recovery_hints().request_rescan();
            self.scans.retain(|scan| current_source(&scan.source));
            self.deferred.retain(|(_, scan)| current_source(&scan.source));
        }
        self.sources = sources;
        self.next_inventory = Some(Instant::now() + INVENTORY_INTERVAL);
    }

    fn defer(&mut self, mut scan: DiskScan) {
        if let Some(buckets) = scan.buckets.as_mut() {
            for bucket in buckets {
                bucket.cursor = None;
                bucket.incarnation = None;
            }
        }
        let same_scope = |other: &DiskScan| {
            scan.source.pool == other.source.pool
                && scan.source.set == other.source.set
                && scan.source.disk.to_string() == other.source.disk.to_string()
                && scan
                    .buckets
                    .as_ref()
                    .and_then(|buckets| buckets.front())
                    .map(|bucket| &bucket.bucket)
                    == other
                        .buckets
                        .as_ref()
                        .and_then(|buckets| buckets.front())
                        .map(|bucket| &bucket.bucket)
        };
        if !self.deferred.iter().any(|(_, pending)| same_scope(pending)) {
            self.deferred.push_back((Instant::now() + INVENTORY_INTERVAL, scan));
        }
    }

    async fn discover_page(&mut self, store: &Arc<ECStore>, deadline: Instant) {
        let hints = store.ctx.delete_recovery_hints();
        let active_cursor = |scan: &DiskScan| {
            scan.buckets
                .as_ref()
                .and_then(|buckets| buckets.front())
                .is_some_and(|bucket| bucket.cursor.is_some() || !bucket.pending.is_empty())
        };
        if self.scans.iter().filter(|scan| active_cursor(scan)).count() >= MAX_ACTIVE_DISCOVERY_CURSORS {
            // Keep room for exact-key recovery and other discovery callers in
            // the disk API's bounded session table. Finish existing cursors
            // before starting another bucket; queued sources retain their place.
            if let Some(index) = self.scans.iter().position(active_cursor) {
                self.scans.rotate_left(index);
            }
        }
        let Some(mut scan) = self.scans.pop_front() else { return };
        if scan.buckets.is_none() {
            match tokio::time::timeout_at(deadline, scan.source.disk.list_volumes()).await {
                Ok(Ok(volumes)) => {
                    scan.buckets = Some(
                        volumes
                            .into_iter()
                            .filter(|volume| !is_meta_bucketname(&volume.name))
                            .map(|volume| BucketScan {
                                bucket: volume.name,
                                incarnation: None,
                                cursor: None,
                                pending: VecDeque::new(),
                            })
                            .collect(),
                    );
                }
                _ => {
                    self.defer(scan);
                    return;
                }
            }
        } else if let Some(buckets) = scan.buckets.as_mut()
            && let Some(mut bucket) = buckets.pop_front()
        {
            let had_pending = !bucket.pending.is_empty();
            admit_discovered_keys(hints, &scan.source, &mut bucket);
            if !bucket.pending.is_empty() || (had_pending && bucket.cursor.is_none()) {
                if !bucket.pending.is_empty() {
                    buckets.push_front(bucket);
                }
                if !buckets.is_empty() {
                    self.scans.push_back(scan);
                }
                return;
            }
            if bucket.incarnation.is_none() {
                match tokio::time::timeout_at(deadline, store.bucket_incarnation_id_from_disk(&bucket.bucket)).await {
                    Ok(Ok(incarnation)) => bucket.incarnation = Some(incarnation),
                    _ => {
                        self.defer(DiskScan {
                            source: scan.source.clone(),
                            buckets: Some(VecDeque::from([bucket])),
                        });
                        if !buckets.is_empty() {
                            self.scans.push_back(scan);
                        }
                        return;
                    }
                }
            }
            match tokio::time::timeout_at(
                deadline,
                scan.source
                    .disk
                    .discover_delete_transactions(&bucket.bucket, None, bucket.cursor.take(), DISCOVERY_PAGE_SIZE),
            )
            .await
            {
                Ok(Ok(page)) => {
                    bucket.pending = page.transactions.into_iter().map(|transaction| transaction.object).collect();
                    bucket.cursor = page.next;
                    admit_discovered_keys(hints, &scan.source, &mut bucket);
                    if bucket.cursor.is_some() || !bucket.pending.is_empty() {
                        // Keep one bounded page until its keys are admitted;
                        // unopened buckets cannot displace its continuation.
                        buckets.push_front(bucket);
                    }
                }
                _ => {
                    // Retry only this source, never a full healthy tree
                    // because another disk is offline or a cursor expired.
                    self.defer(DiskScan {
                        source: scan.source.clone(),
                        buckets: Some(VecDeque::from([bucket])),
                    });
                }
            }
        }
        if scan.buckets.as_ref().is_some_and(|buckets| !buckets.is_empty()) {
            self.scans.push_back(scan);
        }
    }

    async fn discover(&mut self, store: &Arc<ECStore>, cancel: &CancellationToken) {
        // ECStore starts before the bucket metadata system is published. Do
        // not turn that bootstrap state into a deferred disk/bucket failure.
        if store.ctx.bucket_metadata_sys().is_none() {
            return;
        }

        let hints = store.ctx.delete_recovery_hints();
        // Renew only this worker's bounded local cursors before any await.
        // Backpressure and a slow canonical rollback must not erase progress.
        for scan in &mut self.scans {
            if let Some(bucket) = scan.buckets.as_mut().and_then(|buckets| buckets.front_mut())
                && let Some(cursor) = bucket.cursor.as_ref()
            {
                let renewed = match scan.source.disk.as_ref() {
                    crate::disk::Disk::Local(local) => local.get_disk().renew_delete_recovery_cursor(&bucket.bucket, cursor),
                    crate::disk::Disk::Remote(_) => false,
                };
                if !renewed {
                    bucket.cursor = None;
                    hints.request_rescan();
                }
            }
        }
        let discovery_deadline = Instant::now() + DISCOVERY_BUDGET;
        tokio::select! {
            _ = cancel.cancelled() => return,
            result = tokio::time::timeout_at(discovery_deadline, self.refresh_inventory(store)) => {
                if result.is_err() { hints.request_rescan(); }
            }
        }
        if self.scan_generation.is_none() {
            if self.next_sweep.is_some_and(|next| Instant::now() >= next) {
                hints.request_rescan();
                self.next_sweep = None;
            }
            if let Some(generation) = hints.rescan() {
                self.scan_generation = Some(generation);
                self.scans = self
                    .sources
                    .iter()
                    .cloned()
                    .map(|source| DiskScan { source, buckets: None })
                    .collect();
            }
        }
        for _ in 0..self.deferred.len() {
            let Some((ready, scan)) = self.deferred.pop_front() else { break };
            if ready <= Instant::now() {
                self.scans.push_back(scan);
            } else {
                self.deferred.push_back((ready, scan));
            }
        }
        for _ in 0..PAGES_PER_PASS {
            if self.scans.is_empty() || Instant::now() >= discovery_deadline {
                break;
            }
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = self.discover_page(store, discovery_deadline) => {}
            }
        }
        if self.scans.is_empty()
            && let Some(generation) = self.scan_generation.take()
        {
            hints.ack_rescan(generation);
            self.next_sweep = Some(Instant::now() + ANTI_ENTROPY_INTERVAL);
        }
    }

    #[cfg(test)]
    async fn run(&mut self, store: &Arc<ECStore>, cancel: &CancellationToken) {
        self.discover(store, cancel).await;
        recover_hints(store, cancel).await;
    }
}

#[derive(Default, PartialEq, Eq)]
struct RecoveryFailures {
    pending: usize,
    blocked: usize,
    corrupt: usize,
    failed: usize,
}

struct RecoveryWork<'a> {
    store: &'a Arc<ECStore>,
    cancel: &'a CancellationToken,
    tasks: FuturesUnordered<BoxFuture<'a, (Arc<DeleteRecoveryKey>, RecoveryFailures)>>,
    active: HashSet<Arc<DeleteRecoveryKey>>,
    remaining: usize,
    deadline: Instant,
}

impl<'a> RecoveryWork<'a> {
    fn new(store: &'a Arc<ECStore>, cancel: &'a CancellationToken) -> Self {
        Self {
            store,
            cancel,
            tasks: FuturesUnordered::new(),
            active: HashSet::with_capacity(RECOVERY_CONCURRENCY),
            remaining: 0,
            deadline: Instant::now(),
        }
    }

    fn begin_pass(&mut self) {
        self.remaining = self.store.ctx.delete_recovery_hints().pending_len().min(KEYS_PER_PASS);
        self.deadline = Instant::now() + PASS_START_BUDGET;
    }

    fn admit(&mut self) {
        while self.remaining > 0
            && self.active.len() < RECOVERY_CONCURRENCY
            && Instant::now() < self.deadline
            && !self.cancel.is_cancelled()
        {
            let Some(ticket) = self.store.ctx.delete_recovery_hints().take_available(&self.active) else {
                break;
            };
            self.active.insert(ticket.key.clone());
            self.remaining -= 1;
            self.tasks.push(Box::pin(recover_hint(self.store, self.cancel, ticket)));
        }
    }

    async fn next(&mut self) -> Option<RecoveryFailures> {
        let (key, failures) = self.tasks.next().await?;
        self.active.remove(&key);
        Some(failures)
    }
}

impl RecoveryFailures {
    fn add(&mut self, other: Self) {
        self.pending += other.pending;
        self.blocked += other.blocked;
        self.corrupt += other.corrupt;
        self.failed += other.failed;
    }
}

async fn recover_hint(
    store: &Arc<ECStore>,
    cancel: &CancellationToken,
    ticket: DeleteRecoveryTicket,
) -> (Arc<DeleteRecoveryKey>, RecoveryFailures) {
    let key = ticket.key.clone();
    let opts = HealOpts {
        pool: Some(key.pool),
        set: Some(key.set),
        ..Default::default()
    };
    // The admission budget never truncates an admitted rollback. Canonical
    // recovery retains its configured deadline and disk/namespace ownership.
    let outcome = tokio::select! {
        biased;
        _ = cancel.cancelled() => None,
        result = store.recover_delete_transactions_at_incarnation(
            &key.bucket, Some(&key.object), key.incarnation, &opts,
        ) => Some(result),
    };
    let mut failures = RecoveryFailures::default();
    if !matches!(outcome, Some(Ok(_))) {
        match outcome {
            Some(Err(crate::error::StorageError::ErasureReadQuorum)) => failures.pending = 1,
            Some(Err(crate::error::StorageError::SlowDown)) => failures.blocked = 1,
            Some(Err(crate::error::StorageError::FileCorrupt)) => failures.corrupt = 1,
            Some(Err(_)) => failures.failed = 1,
            _ => {}
        }
        store.ctx.delete_recovery_hints().retry(ticket);
    }
    (key, failures)
}

#[cfg(test)]
async fn recover_hints(store: &Arc<ECStore>, cancel: &CancellationToken) -> RecoveryFailures {
    let mut work = RecoveryWork::new(store, cancel);
    work.begin_pass();
    work.admit();
    let mut failures = RecoveryFailures::default();
    while let Some(result) = work.next().await {
        failures.add(result);
        work.admit();
    }
    failures
}

async fn run_recovery(store: &Arc<ECStore>, cancel: &CancellationToken) {
    let mut interval = tokio::time::interval(INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut previous = RecoveryFailures::default();
    let mut failures = RecoveryFailures::default();
    let mut work = RecoveryWork::new(store, cancel);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                // Poll even not-yet-started jobs so each admitted ticket is
                // returned without acknowledging a newer foreground hint.
                while work.next().await.is_some() {}
                return;
            }
            _ = interval.tick() => {
                work.begin_pass();
                if failures != previous {
                    tracing::warn!(
                        event = EVENT_DELETE_RECOVERY_STATUS,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DELETE_RECOVERY,
                        state = "recovery_retry_changed",
                        pending = failures.pending,
                        blocked = failures.blocked,
                        corrupt = failures.corrupt,
                        failed = failures.failed,
                        pending_keys = store.ctx.delete_recovery_hints().pending_len(),
                        "Delete recovery settlement retry state changed"
                    );
                    previous = failures;
                }
                failures = RecoveryFailures::default();
            }
            Some(result) = work.next(), if !work.tasks.is_empty() => failures.add(result),
        }
        work.admit();
    }
}

pub(super) fn start_delete_recovery(store: Arc<ECStore>, cancel: CancellationToken) {
    if !store.ctx.delete_recovery_hints().start(store.id) {
        return;
    }
    tokio::spawn(async move {
        let discovery = async {
            let mut interval = tokio::time::interval(INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut pass = RecoveryPass::default();
            let mut previous_deferred = 0;
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    _ = interval.tick() => {},
                }
                pass.discover(&store, &cancel).await;
                let deferred = pass.deferred.len();
                if deferred != previous_deferred {
                    tracing::warn!(
                        event = EVENT_DELETE_RECOVERY_STATUS,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DELETE_RECOVERY,
                        state = "discovery_retry_changed",
                        deferred_sources = deferred,
                        pending_keys = store.ctx.delete_recovery_hints().pending_len(),
                        "Delete recovery discovery retry state changed"
                    );
                    previous_deferred = deferred;
                }
            }
        };
        let recovery = run_recovery(&store, &cancel);
        // Discovery and independent exact keys keep advancing while a slow
        // rollback retains its configured deadline and ownership fences.
        tokio::join!(discovery, recovery);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
    use crate::disk::{DeleteOptions, DeleteRecoveryAction, STORAGE_FORMAT_FILE};
    use crate::object_api::{ObjectOptions, PutObjReader};
    use crate::storage_api_contracts::bucket::{BucketOperations, MakeBucketOptions};
    use crate::storage_api_contracts::object::{ObjectIO, ObjectOperations};
    use rustfs_filemeta::FileInfo;

    fn key(object: &str) -> DeleteRecoveryKey {
        DeleteRecoveryKey {
            pool: 0,
            set: 0,
            bucket: "bucket".to_owned(),
            object: object.to_owned(),
            incarnation: Some(Uuid::from_u128(1)),
        }
    }

    #[test]
    fn delete_recovery_hints_coalesce_without_losing_fifo_or_new_generations() {
        let hints = DeleteRecoveryHints::with_capacity(2);
        let first = hints.register(key("a")).expect("first hint");
        hints.register(key("b")).expect("second hint");
        let newer = hints.register(key("a")).expect("coalesced hint");
        hints.ack(&first);
        let taken = hints.take(1);
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].key.object, "a", "coalescing must preserve fairness");
        assert_eq!(taken[0].generation, newer.generation);
        let latest = hints.register(key("a")).expect("hint during recovery");
        hints.ack(&taken[0]);
        hints.retry(taken.into_iter().next().expect("selected ticket"));
        let pending = hints.take(2);
        assert_eq!(pending[0].key.object, "b");
        assert_eq!(pending[1].generation, latest.generation, "old recovery cannot replace a newer hint");
    }

    #[test]
    fn delete_recovery_overflow_rearms_after_preparation_and_preserves_new_scan_obligations() {
        let hints = DeleteRecoveryHints::with_capacity(1);
        let overflow = hints.register(key("occupied")).expect("retain request lifetime");
        hints.register(key("later")).expect("evict before preparation");
        let scan = hints.rescan().expect("overflow requests discovery");
        hints.ack_rescan(scan);
        assert!(hints.rescan().is_none());
        drop(overflow);
        let next = hints.rescan().expect("completion after discovery must rearm overflow");
        hints.request_rescan();
        hints.ack_rescan(next);
        assert!(hints.rescan().is_some(), "old scan cannot clear a newer obligation");
        let completed = hints.register(key("completed")).expect("overflow ticket");
        hints.ack(&completed);
        let final_scan = hints.rescan().expect("queued scan");
        hints.ack_rescan(final_scan);
        drop(completed);
        assert!(hints.rescan().is_none(), "synchronous settlement needs no new scan");
    }

    #[test]
    fn delete_recovery_hints_bound_tombstones_and_retry_after_drain() {
        let hints = DeleteRecoveryHints::with_capacity(2);
        for _ in 0..100 {
            let ticket = hints.register(key("same")).expect("register reusable key");
            hints.ack(&ticket);
        }
        let _a = hints.register(key("a")).expect("a");
        hints.register(key("b")).expect("b");
        assert!(hints.state.lock().expect("hint state").order.len() <= 2);
        let mut tickets = hints.take(2);
        assert_eq!(tickets.len(), 2);
        hints.retry(tickets.remove(0));
        assert_eq!(hints.take(2).len(), 1, "failed work survives an empty queue");
    }

    #[test]
    fn delete_recovery_active_key_preserves_newer_hint_and_yields_to_other_keys() {
        let hints = DeleteRecoveryHints::with_capacity(4);
        let original = hints.register(key("active")).expect("original request");
        let admitted = hints.take(1).pop().expect("admit original generation");
        let active = HashSet::from([admitted.key.clone()]);
        let newer = hints.register(key("active")).expect("new foreground generation");
        hints.register(key("later")).expect("independent key");
        let later = hints.take_available(&active).expect("skip the active key");
        assert_eq!(later.key.object, "later");
        assert!(hints.take_available(&active).is_none(), "a newer generation cannot occupy a second slot");
        hints.ack(&original);
        hints.retry(admitted);
        assert!(hints.is_pending(&key("active")), "old success or cancellation cannot clear newer work");
        let next = hints
            .take_available(&HashSet::new())
            .expect("active key becomes eligible after completion");
        assert_eq!(next.generation, newer.generation, "retry cannot overwrite the foreground generation");
    }

    #[test]
    fn delete_recovery_hint_compaction_stays_amortized_near_capacity() {
        let capacity = HINT_CAPACITY;
        let completed = capacity * 4;
        let hints = DeleteRecoveryHints::with_capacity(capacity);
        let blocked: Vec<_> = (0..capacity - 1)
            .map(|index| hints.register(key(&format!("blocked-{index}"))).expect("blocked hint"))
            .collect();
        hints.ack_rescan(hints.rescan().expect("startup discovery"));
        for _ in 0..completed {
            let ticket = hints.register(key("completed")).expect("healthy delete");
            hints.ack(&ticket);
            let state = hints.state.lock().expect("hint state");
            assert_eq!(state.pending.len(), capacity - 1, "healthy completion cannot evict blocked work");
            assert!(state.order.len() <= capacity * 2, "tombstones remain bounded");
        }
        let visits = hints.state.lock().expect("hint state").compaction_visits;
        eprintln!("compaction visited {visits} tickets for {completed} successful deletes near capacity");
        assert!(
            visits <= (completed + capacity) * 2,
            "queue maintenance must be amortized, not capacity per request"
        );
        assert!(hints.rescan().is_none(), "successful deletes must not cause false overflow");
        let remaining = hints.take(capacity);
        assert_eq!(remaining.len(), blocked.len());
        for (actual, expected) in remaining.iter().zip(&blocked) {
            assert_eq!(actual.generation, expected.generation, "compaction preserves FIFO and generations");
        }
    }

    #[test]
    fn delete_recovery_saturated_blocked_hints_cannot_exclude_later_discovery() {
        let hints = DeleteRecoveryHints::with_capacity(2);
        hints.register(key("blocked-a")).expect("first blocked key");
        hints.register(key("blocked-b")).expect("second blocked key");
        for ticket in hints.take(2) {
            hints.retry(ticket);
        }
        hints
            .register(key("healthy-later"))
            .expect("later page can replace oldest hint");
        let next = hints.take(2);
        assert_eq!(next.len(), 2);
        assert!(next.iter().any(|ticket| ticket.key.object == "healthy-later"));
        assert!(hints.rescan().is_some(), "evicted durable work remains a discovery obligation");
    }

    async fn fixture(bucket: &str) -> (Vec<tempfile::TempDir>, Arc<ECStore>, Vec<DiskStore>) {
        let (dirs, store) = isolated_store_over_temp_disks().await;
        // These tests drive discovery and recovery explicitly. Stop the loops
        // started by ECStore::init before adding work or advancing virtual time.
        store.background_cancel_token().expect("fixture background token").cancel();
        tokio::task::yield_now().await;
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create recovery bucket");
        let mut reader = PutObjReader::from_vec(vec![19; 4096]);
        store
            .put_object(
                bucket,
                "object",
                &mut reader,
                &ObjectOptions {
                    write_completion: crate::object_api::WriteCompletion::TailDrained,
                    ..Default::default()
                },
            )
            .await
            .expect("write recovery source");
        let disks = store.pools[0].disk_set[0]
            .disk_inventory()
            .await
            .into_iter()
            .map(|disk| disk.expect("online disk"))
            .collect();
        (dirs, store, disks)
    }

    async fn prepare(store: &Arc<ECStore>, disks: &[DiskStore], bucket: &str) -> Uuid {
        let transaction = Uuid::new_v4();
        let incarnation = store
            .bucket_incarnation_id_from_disk(bucket)
            .await
            .expect("bucket incarnation");
        for disk in disks {
            disk.delete_version_recoverable(
                bucket,
                "object",
                FileInfo {
                    name: "object".to_owned(),
                    ..Default::default()
                },
                false,
                DeleteOptions {
                    old_data_dir: Some(transaction),
                    delete_recovery_bucket_incarnation: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("persist preparation");
        }
        transaction
    }

    async fn publish_one(disk: &DiskStore, bucket: &str, transaction: Uuid) {
        let inspection = disk
            .inspect_delete_transaction(bucket, "object", transaction)
            .await
            .expect("inspect preparation");
        disk.settle_delete_transaction(
            bucket,
            "object",
            transaction,
            inspection.fingerprint,
            DeleteRecoveryAction::PublishCommit,
        )
        .await
        .expect("persist receipt with lost response");
    }

    fn hint_only_pass(store: &Arc<ECStore>) -> RecoveryPass {
        let hints = store.ctx.delete_recovery_hints();
        if let Some(scan) = hints.rescan() {
            hints.ack_rescan(scan);
        }
        RecoveryPass {
            next_inventory: Some(Instant::now() + INVENTORY_INTERVAL),
            ..Default::default()
        }
    }

    async fn register(store: &Arc<ECStore>, bucket: &str) {
        store.ctx.delete_recovery_hints().register(DeleteRecoveryKey {
            bucket: bucket.to_owned(),
            object: "object".to_owned(),
            pool: 0,
            set: 0,
            incarnation: Some(
                store
                    .bucket_incarnation_id_from_disk(bucket)
                    .await
                    .expect("source incarnation"),
            ),
        });
    }

    #[tokio::test]
    async fn delete_recovery_waits_for_metadata_before_consuming_startup_discovery() {
        let (_dirs, store) = isolated_store_over_temp_disks().await;
        store.background_cancel_token().expect("background token").cancel();
        tokio::task::yield_now().await;
        assert!(store.ctx.bucket_metadata_sys().is_none(), "metadata bootstrap has not completed");
        let hints = store.ctx.delete_recovery_hints();
        hints.request_rescan();
        let generation = hints.rescan().expect("startup discovery obligation");
        let mut pass = RecoveryPass::default();
        pass.discover(&store, &CancellationToken::new()).await;
        assert_eq!(hints.rescan(), Some(generation), "bootstrap cannot consume the discovery obligation");
        assert!(pass.deferred.is_empty(), "bootstrap is not a failed disk scope");
        assert!(pass.next_inventory.is_none(), "bootstrap must not delay the initial inventory");
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        pass.discover(&store, &CancellationToken::new()).await;
        assert!(hints.rescan().is_none(), "the first ready pass fulfills the empty-store scan immediately");
        assert!(pass.deferred.is_empty());
    }

    #[tokio::test]
    async fn delete_recovery_clean_startup_discovers_hidden_preparation() {
        let bucket = "runtime-clean-start";
        let (_dirs, store, disks) = fixture(bucket).await;
        prepare(&store, &disks, bucket).await;
        assert!(
            disks[0]
                .read_all(bucket, &format!("object/{STORAGE_FORMAT_FILE}"))
                .await
                .is_err()
        );
        let mut pass = RecoveryPass::default();
        for _ in 0..8 {
            pass.run(&store, &CancellationToken::new()).await;
        }
        assert!(
            disks[0]
                .read_all(bucket, &format!("object/{STORAGE_FORMAT_FILE}"))
                .await
                .is_ok(),
            "startup must restore an uncommitted hidden object without a client DELETE or dirty shutdown"
        );
        assert!(
            store.ctx.delete_recovery_hints().rescan().is_none(),
            "a successful scan is not repeated each tick"
        );
    }

    #[tokio::test]
    async fn delete_recovery_partial_publication_converges_without_another_delete() {
        let bucket = "runtime-lost-response";
        let (_dirs, store, disks) = fixture(bucket).await;
        let mut pass = hint_only_pass(&store);
        register(&store, bucket).await;
        let transaction = prepare(&store, &disks, bucket).await;
        publish_one(&disks[0], bucket, transaction).await;
        pass.run(&store, &CancellationToken::new()).await;
        for disk in &disks {
            let page = disk
                .discover_delete_transactions(bucket, Some("object"), None, 128)
                .await
                .expect("inspect cleanup");
            assert!(
                page.transactions.is_empty(),
                "partial publication must converge through canonical recovery"
            );
        }
        assert!(store.ctx.delete_recovery_hints().take(1).is_empty());
    }

    #[tokio::test]
    async fn delete_recovery_offline_participant_retains_work_until_return() {
        let bucket = "runtime-offline-return";
        let (_dirs, store, disks) = fixture(bucket).await;
        let mut pass = hint_only_pass(&store);
        register(&store, bucket).await;
        let transaction = prepare(&store, &disks, bucket).await;
        publish_one(&disks[0], bucket, transaction).await;
        let set = &store.pools[0].disk_set[0];
        let offline = set.disks.write().await[3].take();
        pass.run(&store, &CancellationToken::new()).await;
        assert_eq!(
            store
                .ctx
                .delete_recovery_hints()
                .state
                .lock()
                .expect("pending state")
                .pending
                .len(),
            1
        );
        set.disks.write().await[3] = offline;
        pass.run(&store, &CancellationToken::new()).await;
        assert!(
            store.ctx.delete_recovery_hints().take(1).is_empty(),
            "returning participant releases retained work"
        );
        assert!(
            disks[0]
                .discover_delete_transactions(bucket, Some("object"), None, 128)
                .await
                .expect("reclaimed journals")
                .transactions
                .is_empty()
        );
    }

    #[tokio::test]
    async fn delete_recovery_raw_discovery_is_bounded_and_rotates_disks_and_buckets() {
        let bucket = "runtime-page-budget";
        let (_dirs, store, _disks) = fixture(bucket).await;
        for index in 0..8 {
            store
                .make_bucket(&format!("runtime-other-{index}"), &MakeBucketOptions::default())
                .await
                .expect("create scan bucket");
        }
        let mut pass = RecoveryPass::default();
        pass.run(&store, &CancellationToken::new()).await;
        assert_eq!(pass.scans.len(), 4, "one pass cannot consume all bucket pages");
        let remaining: Vec<_> = pass
            .scans
            .iter()
            .map(|scan| scan.buckets.as_ref().expect("initialized disk").len())
            .collect();
        assert!(
            remaining.iter().all(|count| *count == 8),
            "each disk receives one page fairly after volume discovery"
        );
        for _ in 0..8 {
            pass.run(&store, &CancellationToken::new()).await;
        }
        assert!(pass.scans.is_empty());
        assert!(store.ctx.delete_recovery_hints().rescan().is_none());
    }
    #[tokio::test]
    async fn delete_recovery_blocked_capacity_yields_until_periodic_sweep() {
        let (_dirs, store, _disks) = fixture("runtime-blocked-sweep").await;
        let hints = store.ctx.delete_recovery_hints();
        hints.request_rescan();
        let scanning = hints.rescan().expect("startup discovery obligation");
        for index in 0..HINT_CAPACITY {
            assert!(hints.register_discovered(key(&format!("blocked-{index}"))));
        }
        for ticket in hints.take(KEYS_PER_PASS) {
            hints.retry(ticket);
        }
        for index in 0..KEYS_PER_PASS {
            assert!(
                hints.register_discovered(key(&format!("later-page-{index}"))),
                "attempted failures yield to later discovery"
            );
        }
        let in_flight = hints.take(1).pop().expect("one admitted recovery attempt");
        assert!(hints.register_discovered(key("arrived-during-recovery")));
        hints.retry(in_flight);
        let mut pass = RecoveryPass {
            next_inventory: Some(Instant::now() + ANTI_ENTROPY_INTERVAL),
            scan_generation: Some(scanning),
            ..Default::default()
        };
        pass.discover(&store, &CancellationToken::new()).await;
        assert!(
            hints.rescan().is_none(),
            "attempted failures cannot invalidate the completed scan generation"
        );
        let scheduled = pass.next_sweep.expect("periodic discovery remains scheduled");
        assert!(scheduled >= Instant::now() + ANTI_ENTROPY_INTERVAL - Duration::from_secs(1));
        pass.discover(&store, &CancellationToken::new()).await;
        assert!(pass.scan_generation.is_none());
        assert_eq!(pass.next_sweep, Some(scheduled), "the next tick must wait for the hourly sweep");
        hints
            .register(key("foreground-new-request"))
            .expect("foreground overflow remains tracked");
        let foreground = hints
            .rescan()
            .expect("foreground eviction still requests immediate discovery");
        assert_ne!(foreground, scanning);
        hints.ack_rescan(scanning);
        assert_eq!(hints.rescan(), Some(foreground), "an older scan cannot clear a foreground obligation");
    }

    #[tokio::test]
    async fn delete_recovery_admission_stays_within_each_tick_budget() {
        let bucket = "runtime-admission-budget";
        let (_dirs, store, _disks) = fixture(bucket).await;
        let incarnation = store.bucket_incarnation_id_from_disk(bucket).await.expect("bucket identity");
        let hints = store.ctx.delete_recovery_hints();
        let total = KEYS_PER_PASS * 3;
        for index in 0..total {
            hints.register(DeleteRecoveryKey {
                pool: 0,
                set: 0,
                bucket: bucket.to_owned(),
                object: format!("absent-{index}"),
                incarnation: Some(incarnation),
            });
        }
        for pass in 1..=3 {
            let failures = recover_hints(&store, &CancellationToken::new()).await;
            assert!(failures == RecoveryFailures::default(), "absent exact keys settle successfully");
            assert_eq!(
                hints.pending_len(),
                total - pass * KEYS_PER_PASS,
                "only the current admission budget may leave the queue"
            );
        }
    }

    #[tokio::test]
    async fn delete_recovery_cancel_returns_every_unpolled_admission() {
        let bucket = "runtime-cancel-admission";
        let (_dirs, store, _disks) = fixture(bucket).await;
        let hints = store.ctx.delete_recovery_hints();
        for index in 0..KEYS_PER_PASS {
            hints.register(key(&format!("queued-{index}")));
        }
        let cancel = CancellationToken::new();
        let mut work = RecoveryWork::new(&store, &cancel);
        work.begin_pass();
        work.admit();
        assert_eq!(work.active.len(), RECOVERY_CONCURRENCY, "admission obeys the concurrency cap");
        assert_eq!(hints.pending_len(), KEYS_PER_PASS - RECOVERY_CONCURRENCY);
        let newer = hints.register(key("queued-0")).expect("foreground request during admission");
        cancel.cancel();
        while let Some(result) = work.next().await {
            assert!(result == RecoveryFailures::default(), "cancellation is not a storage failure");
            work.admit();
        }
        assert!(work.active.is_empty());
        assert_eq!(hints.pending_len(), KEYS_PER_PASS, "every admitted key is retained exactly once");
        let tickets = hints.take(KEYS_PER_PASS);
        assert_eq!(
            tickets
                .iter()
                .find(|ticket| ticket.key.object == "queued-0")
                .expect("newer hint")
                .generation,
            newer.generation
        );
    }

    #[tokio::test]
    async fn delete_recovery_slow_key_does_not_block_later_batches() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("60"))], async {
            let bucket = "runtime-independent-keys";
            let (_dirs, store, disks) = fixture(bucket).await;
            prepare(&store, &disks, bucket).await;
            let incarnation = store.bucket_incarnation_id_from_disk(bucket).await.expect("bucket identity");
            let lock = store.handle_new_ns_lock(bucket, "slow-key").await.expect("slow key lock");
            let held = lock
                .get_write_lock(Duration::from_secs(60))
                .await
                .expect("hold only the slow key");
            let hints = store.ctx.delete_recovery_hints();
            for object in std::iter::once("slow-key".to_owned())
                .chain((0..KEYS_PER_PASS * 2).map(|index| format!("healthy-{index}")))
                .chain(std::iter::once("object".to_owned()))
            {
                hints.register(DeleteRecoveryKey {
                    pool: 0,
                    set: 0,
                    bucket: bucket.to_owned(),
                    object,
                    incarnation: Some(incarnation),
                });
            }
            let cancel = CancellationToken::new();
            let worker_store = store.clone();
            let worker_cancel = cancel.clone();
            let worker = tokio::spawn(async move { run_recovery(&worker_store, &worker_cancel).await });
            let started = Instant::now();
            let recovered = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let restored = futures::future::join_all(disks.iter().map(|disk| async move {
                        disk.read_all(bucket, &format!("object/{STORAGE_FORMAT_FILE}")).await.is_ok()
                    }))
                    .await;
                    if restored.into_iter().all(|restored| restored) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await;
            eprintln!(
                "healthy rollback behind {} keys completed={} after {:.3}s while the first key stayed locked",
                KEYS_PER_PASS * 2 + 1,
                recovered.is_ok(),
                started.elapsed().as_secs_f64()
            );
            cancel.cancel();
            worker.await.expect("cancel every admitted recovery");
            drop(held);
            assert!(recovered.is_ok(), "one locked key must not block healthy work beyond the first batch");
            let mut reader = store
                .get_object_reader(bucket, "object", None, http::HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("read recovered source");
            let mut bytes = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut reader.stream, &mut bytes)
                .await
                .expect("read complete source bytes");
            assert_eq!(bytes, vec![19; 4096], "healthy rollback preserves complete original bytes");
            for disk in &disks {
                assert!(
                    disk.read_all(bucket, &format!("object/{STORAGE_FORMAT_FILE}")).await.is_ok(),
                    "rollback restores each participant"
                );
            }
            assert!(
                hints.is_pending(&DeleteRecoveryKey {
                    pool: 0,
                    set: 0,
                    bucket: bucket.to_owned(),
                    object: "slow-key".to_owned(),
                    incarnation: Some(incarnation),
                }),
                "cancellation retains the slow key for retry"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn delete_recovery_slow_first_key_preserves_unattempted_backpressure() {
        let bucket = "runtime-slow-first";
        let (_dirs, store, _disks) = fixture(bucket).await;
        let incarnation = store.bucket_incarnation_id_from_disk(bucket).await.expect("bucket identity");
        let hints = store.ctx.delete_recovery_hints();
        for index in 0..HINT_CAPACITY {
            hints.register(DeleteRecoveryKey {
                pool: 0,
                set: 0,
                bucket: bucket.to_owned(),
                object: format!("waiting-{index}"),
                incarnation: Some(incarnation),
            });
        }
        let gate = store.ctx.data_movement_operation_gate();
        let held = gate.write().await;
        tokio::time::pause();
        let worker_store = store.clone();
        let worker = tokio::spawn(async move { recover_hints(&worker_store, &CancellationToken::new()).await });
        for _ in 0..100 {
            if hints.pending_len() == HINT_CAPACITY - RECOVERY_CONCURRENCY {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            hints.pending_len(),
            HINT_CAPACITY - RECOVERY_CONCURRENCY,
            "only admitted keys leave the queue"
        );
        for index in 0..RECOVERY_CONCURRENCY {
            assert!(
                hints.register_discovered(key(&format!("newly-discovered-{index}"))),
                "one in-flight key releases one slot"
            );
        }
        assert!(!hints.register_discovered(key("must-wait")), "untouched keys must apply backpressure");
        tokio::time::advance(PASS_START_BUDGET + Duration::from_secs(1)).await;
        drop(held);
        worker.await.expect("finish the admitted operation");
        tokio::time::resume();
        let state = hints.state.lock().expect("retained pending work");
        for index in RECOVERY_CONCURRENCY..HINT_CAPACITY {
            let queued = state
                .pending
                .iter()
                .find(|(key, _)| key.object == format!("waiting-{index}"))
                .expect("unattempted key remains queued")
                .1;
            assert!(!queued.2, "budget exhaustion must not grant discovery permission to evict untouched work");
        }
    }

    #[test]
    fn delete_recovery_backpressure_eventually_attempts_every_healthy_key() {
        for total in [4_096, 16_384] {
            let hints = DeleteRecoveryHints::with_capacity(HINT_CAPACITY);
            let mut recovered = HashSet::new();
            let mut next = 0usize;
            let healthy = total - total / 1_024 * KEYS_PER_PASS;
            for _ in 0..(total / KEYS_PER_PASS * 4) {
                for _ in 0..(PAGES_PER_PASS * usize::try_from(DISCOVERY_PAGE_SIZE).expect("page size")) {
                    if !recovered.contains(&next) && !hints.register_discovered(key(&next.to_string())) {
                        break;
                    }
                    next = (next + 1) % total;
                }
                for ticket in hints.take(KEYS_PER_PASS) {
                    let index: usize = ticket.key.object.parse().expect("numeric test key");
                    if index % 1_024 < KEYS_PER_PASS {
                        hints.retry(ticket);
                    } else {
                        recovered.insert(index);
                    }
                }
                assert!(hints.pending_len() <= HINT_CAPACITY);
                if recovered.len() == healthy {
                    break;
                }
            }
            assert_eq!(
                recovered.len(),
                healthy,
                "permanent failures at every page-group boundary cannot starve later keys"
            );
        }
    }

    #[tokio::test]
    async fn delete_recovery_real_single_and_batch_delete_register_and_ack_cleanup() {
        for batch in [false, true] {
            for held_snapshot in [false, true] {
                let bucket = "runtime-real-delete";
                let (_dirs, store, disks) = fixture(bucket).await;
                let mut reader = PutObjReader::from_vec(vec![37; 1024 * 1024]);
                store
                    .put_object(
                        bucket,
                        "object",
                        &mut reader,
                        &ObjectOptions {
                            write_completion: crate::object_api::WriteCompletion::TailDrained,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("external source for snapshot cleanup");
                let _pass = hint_only_pass(&store);
                let mut leases = Vec::new();
                if held_snapshot {
                    for disk in &disks {
                        let info = disk
                            .read_version("", bucket, "object", "", &crate::disk::ReadOptions::default())
                            .await
                            .expect("source data directory");
                        let path = format!("object/{}", info.data_dir.expect("external payload"));
                        let lease = disk.acquire_snapshot_lease(bucket, &path).await.expect("protect cleanup");
                        leases.push((disk.clone(), path, lease));
                    }
                }
                if batch {
                    let (_, errors) = store
                        .delete_objects(
                            bucket,
                            vec![crate::storage_api_contracts::object::ObjectToDelete {
                                object_name: "object".to_owned(),
                                ..Default::default()
                            }],
                            ObjectOptions::default(),
                        )
                        .await;
                    assert!(errors.iter().all(Option::is_none), "logical batch delete succeeds: {errors:?}");
                } else {
                    store
                        .delete_object(bucket, "object", ObjectOptions::default())
                        .await
                        .expect("logical single delete succeeds");
                }
                assert_eq!(
                    store.ctx.delete_recovery_hints().pending_len(),
                    usize::from(held_snapshot),
                    "the real preparation hook retains cleanup; synchronous settlement acknowledges its ticket"
                );
                if held_snapshot {
                    for (disk, path, lease) in leases {
                        disk.release_snapshot_lease(bucket, &path, lease)
                            .await
                            .expect("release deferred payload");
                    }
                    recover_hints(&store, &CancellationToken::new()).await;
                    assert_eq!(
                        store.ctx.delete_recovery_hints().pending_len(),
                        0,
                        "maintenance finishes the real DELETE without manual registration"
                    );
                }
                for disk in &disks {
                    assert!(
                        disk.discover_delete_transactions(bucket, Some("object"), None, 128)
                            .await
                            .expect("no residual delete journal")
                            .transactions
                            .is_empty()
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn delete_recovery_continues_current_bucket_before_opening_later_buckets() {
        let bucket = "runtime-cursor-progress";
        let (dirs, store, disks) = fixture(bucket).await;
        let other = "runtime-after-large";
        store
            .make_bucket(other, &MakeBucketOptions::default())
            .await
            .expect("next bucket");
        for index in 0..4_130 {
            std::fs::write(dirs[0].path().join(bucket).join(format!("filler-{index}")), []).expect("raw discovery visit budget");
        }
        let mut pass = RecoveryPass::default();
        pass.scans.push_back(DiskScan {
            source: LocalSource {
                pool: 0,
                set: 0,
                disk: disks[0].clone(),
            },
            buckets: Some(VecDeque::from([bucket, other].map(|bucket| BucketScan {
                bucket: bucket.to_owned(),
                incarnation: None,
                cursor: None,
                pending: VecDeque::new(),
            }))),
        });
        pass.discover_page(&store, Instant::now() + Duration::from_secs(30)).await;
        let first = pass.scans[0]
            .buckets
            .as_ref()
            .expect("pending buckets")
            .front()
            .expect("current bucket");
        assert_eq!(first.bucket, bucket);
        assert!(first.cursor.is_some(), "raw entry budget must leave a continuation");
        pass.discover_page(&store, Instant::now() + Duration::from_secs(30)).await;
        assert_eq!(
            pass.scans[0]
                .buckets
                .as_ref()
                .expect("remaining bucket")
                .front()
                .expect("next bucket")
                .bucket,
            other,
            "the second page completes instead of cycling through unopened buckets"
        );
    }
}
