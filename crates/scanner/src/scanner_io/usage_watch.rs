// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Bounded, session-local mutation witnesses for a complete bucket sweep.
//! These witnesses do not acknowledge dirty work or certify a published root.
use super::*;
use crate::scanner::{LOCAL_SCANNER_ACTIVITY_NODE, ScannerActivitySnapshot, probe_scanner_activity};
use crate::storage_api::scanner_io::{
    EndpointServerPools, ScannerUsageWatchKnown, ScannerUsageWatchObservation, ScannerUsageWatchRequest,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const WATCH_PROOF_VERSION: u16 = 1;
const MAX_WATCH_OWNERS: usize = 64;
const MAX_WATCHES: usize = 256;
const MAX_WATCH_NAME_BYTES: usize = 16 * 1024;
const MAX_WATCH_PROOF_BYTES: usize = 8 * 1024;
const WATCH_LEASE_SAFETY_MARGIN: Duration = Duration::from_millis(250);
const WATCH_QUERY_TIMEOUT: Duration = Duration::from_secs(10);
const WATCH_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);
const WATCH_KEEPALIVE_QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const WATCH_KEEPALIVE_CONCURRENCY: usize = 4;

/// The original revision is persisted with the traversal checkpoint. Renewal
/// may extend a local deadline, but never replaces this starting revision.
/// Owners, rather than leader-relative host names, make the proof portable to
/// a remote disk worker. Every verifier resolves the complete current peer set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScannerUsageWatchProof {
    pub(crate) version: u16,
    pub(crate) bucket: String,
    pub(crate) bucket_incarnation: Uuid,
    pub(crate) structure: DataUsageScanPlanDigest,
    pub(crate) peers: BTreeMap<Uuid, ScannerUsageWatchKnown>,
}

impl ScannerUsageWatchProof {
    pub(crate) fn is_well_formed(&self) -> bool {
        self.version == WATCH_PROOF_VERSION
            && !self.bucket.is_empty()
            && self.bucket != DATA_USAGE_ROOT
            && self.bucket.len() <= 63
            && !self.bucket_incarnation.is_nil()
            && self.structure.0 != [0; 32]
            && !self.peers.is_empty()
            && self.peers.len() <= MAX_WATCH_OWNERS
            && self.peers.iter().all(|(owner, peer)| {
                *owner == peer.owner_id
                    && peer.is_well_formed()
                    && peer.inflight == 0
                    && peer.bucket_incarnation == self.bucket_incarnation
            })
            && rmp_serde::to_vec_named(self).is_ok_and(|encoded| encoded.len() <= MAX_WATCH_PROOF_BYTES)
    }

    pub(crate) fn digest(&self) -> DataUsageScanPlanDigest {
        let mut hash = Sha256::new();
        hash.update(b"scanner-usage-watch-start-v1");
        hash.update(self.version.to_be_bytes());
        hash.update((self.bucket.len() as u64).to_be_bytes());
        hash.update(self.bucket.as_bytes());
        hash.update(self.bucket_incarnation.as_bytes());
        hash.update(self.structure.0);
        hash.update((self.peers.len() as u64).to_be_bytes());
        for (owner, peer) in &self.peers {
            hash.update(owner.as_bytes());
            hash.update(peer.session_id.as_bytes());
            hash.update(peer.watch_id.as_bytes());
            hash.update(peer.bucket_incarnation.as_bytes());
            hash.update(peer.revision.to_be_bytes());
            hash.update(peer.inflight.to_be_bytes());
            hash.update(peer.coverage_version.to_be_bytes());
            hash.update(peer.coverage_epoch.to_be_bytes());
        }
        DataUsageScanPlanDigest(hash.finalize().into())
    }

    /// Revalidates every expected owner and the original revision. An expired
    /// token is queried with Some(token), so the receiver cannot recreate it.
    pub(crate) async fn verify_with_store(&self, store: &ECStore) -> bool {
        if !self.is_well_formed() {
            return false;
        }
        tokio::time::timeout(WATCH_QUERY_TIMEOUT, query_watch(store, &self.bucket, self.bucket_incarnation, Some(self)))
            .await
            .ok()
            .flatten()
            .is_some()
    }

    async fn release_with_store(&self, store: &ECStore) -> bool {
        let request = |known: &ScannerUsageWatchKnown| ScannerUsageWatchRequest {
            bucket: self.bucket.clone(),
            bucket_incarnation: self.bucket_incarnation,
            watch_id: Some(known.watch_id),
            session_id: Some(known.session_id),
            release: true,
        };
        if !self.revoke_local(store) {
            return false;
        }
        if self.peers.len() == 1 {
            return true;
        }
        let Some(notifications) = store.notification_system() else {
            return false;
        };
        let Ok(snapshots) = notifications.scanner_dirty_usage_snapshots().await else {
            return false;
        };
        let mut targets = Vec::new();
        let mut found = HashSet::from([store.id]);
        for (host, snapshot) in snapshots {
            let Ok(owner) = Uuid::parse_str(&snapshot.owner_id) else {
                return false;
            };
            if let Some(peer) = self.peers.get(&owner) {
                if !found.insert(owner) {
                    return false;
                }
                targets.push((host, owner, vec![request(peer)]));
            }
        }
        if found.len() != self.peers.len() {
            return false;
        }
        notifications.scanner_usage_watches(targets).await.is_ok()
    }

    fn revoke_local(&self, store: &ECStore) -> bool {
        let Some(local) = self.peers.get(&store.id) else {
            return false;
        };
        store.scanner_release_usage_watch(&ScannerUsageWatchRequest {
            bucket: self.bucket.clone(),
            bucket_incarnation: self.bucket_incarnation,
            watch_id: Some(local.watch_id),
            session_id: Some(local.session_id),
            release: true,
        });
        true
    }

    #[cfg(test)]
    pub(crate) async fn install_for_test(store: &ECStore, bucket: &str) -> Option<Self> {
        let incarnation = store.bucket_incarnation_id_from_disk(bucket).await.ok()?;
        query_watch(store, bucket, incarnation, None).await.map(|live| live.start)
    }
}

#[derive(Clone)]
struct LiveUsageWatch {
    start: ScannerUsageWatchProof,
    deadline: Instant,
}

type UsageWatchSlot = Arc<Mutex<Option<LiveUsageWatch>>>;

/// Admission follows the existing service cohort's order. Unfinished live
/// watches keep their slot; completed or invalid watches release it. Neither
/// inventory size nor mutations can grow this map beyond its two hard bounds.
#[derive(Default)]
pub(crate) struct ScannerUsageWatchCohort {
    slots: HashMap<String, UsageWatchSlot>,
    name_bytes: usize,
    keepalive: Option<AbortOnDropHandle<()>>,
    owner: Option<std::sync::Weak<ECStore>>,
    keepalive_owner: Option<CancellationToken>,
}

impl ScannerUsageWatchCohort {
    fn admit(&mut self, bucket: &str) -> Option<UsageWatchSlot> {
        self.slots
            .retain(|_, slot| Arc::strong_count(slot) > 1 || slot.try_lock().map_or(true, |state| state.is_some()));
        self.name_bytes = self.slots.keys().map(String::len).sum();
        if let Some(slot) = self.slots.get(bucket) {
            return Some(slot.clone());
        }
        if bucket.is_empty()
            || bucket.len() > 63
            || self.slots.len() >= MAX_WATCHES
            || self.name_bytes.saturating_add(bucket.len()) > MAX_WATCH_NAME_BYTES
        {
            return None;
        }
        let slot = Arc::new(Mutex::new(None));
        self.name_bytes += bucket.len();
        self.slots.insert(bucket.to_owned(), slot.clone());
        Some(slot)
    }

    #[cfg(test)]
    fn retire_completed(&mut self, completed: &HashSet<String>) {
        self.slots.retain(|bucket, _| !completed.contains(bucket));
        self.name_bytes = self.slots.keys().map(String::len).sum();
    }
}

pub(super) async fn admit_scanner_usage_watch(
    cohort: &Arc<StdMutex<ScannerUsageWatchCohort>>,
    store: &Arc<ECStore>,
    owner_ctx: &CancellationToken,
    bucket: &str,
    incarnation: Uuid,
) -> Option<ScannerUsageWatchProof> {
    let slot = {
        let mut state = cohort.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if !start_watch_keepalive(cohort, &mut state, store, owner_ctx) {
            return None;
        }
        state.admit(bucket)?
    };
    // Only equal bucket names serialize their RPC. The bounded cohort mutex
    // is never held across an await or a network operation.
    let mut current = slot.lock().await;
    if current
        .as_ref()
        .is_some_and(|live| live.deadline <= Instant::now() || live.start.bucket_incarnation != incarnation)
    {
        if let Some(expired) = current.take() {
            expired.start.revoke_local(store);
            let _ = tokio::time::timeout(WATCH_QUERY_TIMEOUT, expired.start.release_with_store(store)).await;
        }
    }
    let previous = current
        .as_ref()
        .filter(|live| live.deadline > Instant::now() && live.start.bucket_incarnation == incarnation);
    let result = tokio::time::timeout(
        WATCH_QUERY_TIMEOUT,
        query_watch(store, bucket, incarnation, previous.map(|live| &live.start)),
    )
    .await
    .ok()
    .flatten();
    match result {
        Some(live) => {
            let proof = live.start.clone();
            *current = Some(live);
            Some(proof)
        }
        None => {
            // A changed starting proof can no longer certify any continuation.
            // Release its exact old token so repeated failed admissions cannot
            // renew an invalid hot watch forever and starve other buckets.
            if let Some(previous) = current.as_ref() {
                previous.start.revoke_local(store);
                let _ = tokio::time::timeout(WATCH_QUERY_TIMEOUT, previous.start.release_with_store(store)).await;
            }
            *current = None;
            None
        }
    }
}

/// One driver per leader cohort, with weak ownership in both directions. At
/// most 256 queries run in groups of four, each bounded by five seconds: even
/// a full slow cohort is revisited within the ten-minute producer lease.
/// Retained slots include checkpoints between cycles and completed sources
/// waiting for the root publication/ACK barrier.
fn start_watch_keepalive(
    cohort: &Arc<StdMutex<ScannerUsageWatchCohort>>,
    state: &mut ScannerUsageWatchCohort,
    store: &Arc<ECStore>,
    owner_ctx: &CancellationToken,
) -> bool {
    start_watch_keepalive_with_interval(cohort, state, store, owner_ctx, WATCH_KEEPALIVE_INTERVAL)
}

fn start_watch_keepalive_with_interval(
    cohort: &Arc<StdMutex<ScannerUsageWatchCohort>>,
    state: &mut ScannerUsageWatchCohort,
    store: &Arc<ECStore>,
    owner_ctx: &CancellationToken,
    interval: Duration,
) -> bool {
    if owner_ctx.is_cancelled() {
        return false;
    }
    if let Some(owner) = &state.owner {
        if !owner.upgrade().is_some_and(|owner| Arc::ptr_eq(&owner, store)) {
            return false;
        }
        if state.keepalive_owner.as_ref().is_some_and(|owner| !owner.is_cancelled())
            && state.keepalive.as_ref().is_some_and(|task| !task.is_finished())
        {
            return true;
        }
        // A source-cycle owner may end while its leader-local cohort remains
        // scheduled for another cycle. Resume the keeper for this same store;
        // admission still checks the original token and conservative deadline.
        // Dropping the previous handle cancels any remaining old query first.
        state.keepalive = None;
    }
    let weak_cohort = Arc::downgrade(cohort);
    let weak_store = Arc::downgrade(store);
    state.owner = Some(weak_store.clone());
    state.keepalive_owner = Some(owner_ctx.clone());
    let owner_ctx = owner_ctx.clone();
    state.keepalive = Some(AbortOnDropHandle::new(tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = owner_ctx.cancelled() => return,
                _ = tokio::time::sleep(interval) => {},
            }
            let Some(cohort) = weak_cohort.upgrade() else {
                return;
            };
            let slots = cohort
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .slots
                .values()
                .cloned()
                .collect::<Vec<_>>();
            drop(cohort);
            let Some(store) = weak_store.upgrade() else {
                return;
            };
            tokio::select! {
                _ = owner_ctx.cancelled() => return,
                _ = renew_watch_slots(&store, slots) => {},
            }
            // No strong cohort/store reference survives the next sleep.
        }
    })));
    true
}

async fn renew_watch_slots(store: &Arc<ECStore>, slots: Vec<UsageWatchSlot>) {
    use futures::StreamExt as _;
    futures::stream::iter(slots)
        .for_each_concurrent(WATCH_KEEPALIVE_CONCURRENCY, |slot| async move {
            // Admission already renews while holding this same lock. Never
            // queue behind another query and lose the cohort's time bound.
            let Ok(mut current) = slot.try_lock() else {
                return;
            };
            let Some(previous) = current.as_ref() else {
                return;
            };
            let refreshed = if previous.deadline > Instant::now() {
                tokio::time::timeout(
                    WATCH_KEEPALIVE_QUERY_TIMEOUT,
                    query_watch(store, &previous.start.bucket, previous.start.bucket_incarnation, Some(&previous.start)),
                )
                .await
                .ok()
                .flatten()
            } else {
                None
            };
            if refreshed.is_none() {
                // Every proof contains this owner's exact token. Revoking it
                // synchronously makes Unknown/expiry sticky even if peers
                // become reachable before their remaining leases expire.
                previous.start.revoke_local(store);
            }
            *current = refreshed;
        })
        .await;
}

/// Owns only finished sweeps. Finish after root persistence/ACK; cancellation
/// releases exact tokens best-effort, while lost RPCs retain their bounded TTL.
pub(crate) struct ScannerUsageWatchReleaseGuard {
    store: Option<Arc<ECStore>>,
    proofs: Vec<ScannerUsageWatchProof>,
    cohort: Option<Arc<StdMutex<ScannerUsageWatchCohort>>>,
}

impl ScannerUsageWatchReleaseGuard {
    pub(crate) fn new(store: Option<Arc<ECStore>>, proofs: Vec<ScannerUsageWatchProof>) -> Self {
        Self {
            store,
            proofs,
            cohort: None,
        }
    }

    pub(crate) fn with_cohort(mut self, cohort: Option<Arc<StdMutex<ScannerUsageWatchCohort>>>) -> Self {
        self.cohort = cohort;
        self
    }

    pub(crate) async fn finish(mut self) -> bool {
        let proofs = std::mem::take(&mut self.proofs);
        let cohort = self.cohort.take();
        match self.store.take() {
            Some(store) => release_finished_watches(store, proofs, cohort).await,
            None => proofs.is_empty(),
        }
    }
}

impl Drop for ScannerUsageWatchReleaseGuard {
    fn drop(&mut self) {
        if self.proofs.is_empty() {
            return;
        }
        let Some(store) = self.store.take() else {
            return;
        };
        let proofs = std::mem::take(&mut self.proofs);
        let cohort = self.cohort.take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = release_finished_watches(store, proofs, cohort).await;
            });
        }
    }
}

async fn release_finished_watches(
    store: Arc<ECStore>,
    proofs: Vec<ScannerUsageWatchProof>,
    cohort: Option<Arc<StdMutex<ScannerUsageWatchCohort>>>,
) -> bool {
    use futures::StreamExt as _;
    for proof in &proofs {
        proof.revoke_local(&store);
    }
    tokio::time::timeout(WATCH_QUERY_TIMEOUT, async move {
        if let Some(cohort) = cohort {
            retire_finished_slots(&cohort, &proofs).await;
        }
        let released = futures::stream::iter(proofs)
            .map(|proof| {
                let store = store.clone();
                async move { proof.release_with_store(&store).await }
            })
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await;
        released.into_iter().all(|released| released)
    })
    .await
    .unwrap_or(false)
}

async fn retire_finished_slots(cohort: &Arc<StdMutex<ScannerUsageWatchCohort>>, proofs: &[ScannerUsageWatchProof]) {
    use futures::StreamExt as _;
    futures::stream::iter(proofs)
        .for_each_concurrent(WATCH_KEEPALIVE_CONCURRENCY, |proof| async move {
            let slot = cohort
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .slots
                .get(&proof.bucket)
                .cloned();
            let Some(slot) = slot else {
                return;
            };
            let mut current = slot.lock().await;
            if current.as_ref().is_some_and(|live| live.start.digest() != proof.digest()) {
                return;
            }
            *current = None;
            let mut state = cohort.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if state
                .slots
                .get(&proof.bucket)
                .is_some_and(|stored| Arc::ptr_eq(stored, &slot))
            {
                state.slots.remove(&proof.bucket);
                state.name_bytes = state.slots.keys().map(String::len).sum();
            }
        })
        .await;
}

fn owner_routes(
    local_owner: Uuid,
    activity: &ScannerActivitySnapshot,
    snapshots: Vec<(String, EcstoreScannerPeerDirtyUsageSnapshot)>,
) -> Option<BTreeMap<Uuid, Option<String>>> {
    if local_owner.is_nil() || !activity.contains_key(LOCAL_SCANNER_ACTIVITY_NODE) || activity.len() > MAX_WATCH_OWNERS {
        return None;
    }
    let expected_hosts = activity
        .keys()
        .filter(|host| host.as_str() != LOCAL_SCANNER_ACTIVITY_NODE)
        .cloned()
        .collect::<BTreeSet<_>>();
    if snapshots.len() != expected_hosts.len() {
        return None;
    }
    let mut routes = BTreeMap::from([(local_owner, None)]);
    let mut seen = BTreeSet::new();
    for (host, snapshot) in snapshots {
        let (instance_id, _, _) = crate::scanner::scanner_activity_dirty_usage_state_for_host(activity, &host)?;
        let owner = Uuid::parse_str(&snapshot.owner_id).ok()?;
        // A full dirty bucket list is deliberately not required: overflow in
        // that independent protocol cannot be interpreted as an empty list.
        if !expected_hosts.contains(&host)
            || !seen.insert(host.clone())
            || owner.is_nil()
            || snapshot.instance_id != instance_id
            || routes.insert(owner, Some(host)).is_some()
        {
            return None;
        }
    }
    (seen == expected_hosts).then_some(routes)
}

fn accept_watch_observations(
    bucket: &str,
    incarnation: Uuid,
    routes: &BTreeMap<Uuid, Option<String>>,
    observations: BTreeMap<Uuid, Vec<ScannerUsageWatchObservation>>,
    previous: Option<&ScannerUsageWatchProof>,
    structure: DataUsageScanPlanDigest,
    started: Instant,
    now: Instant,
) -> Option<LiveUsageWatch> {
    if routes.is_empty() || observations.len() != routes.len() {
        return None;
    }
    if let Some(previous) = previous
        && (!previous.is_well_formed()
            || previous.bucket != bucket
            || previous.bucket_incarnation != incarnation
            || previous.structure != structure
            || previous.peers.keys().ne(routes.keys()))
    {
        return None;
    }
    let mut peers = BTreeMap::new();
    let mut deadline = None;
    for owner in routes.keys() {
        let values = observations.get(owner)?;
        if values.len() != 1 || values[0].bucket != bucket {
            return None;
        }
        let known = values[0].known.as_ref()?;
        if known.owner_id != *owner
            || known.bucket_incarnation != incarnation
            || !known.is_well_formed()
            || known.inflight != 0
            || previous.is_some_and(|proof| proof.peers.get(owner).is_none_or(|before| !known.unchanged_since(before)))
        {
            return None;
        }
        // Charge the full request RTT and a margin against the advertised
        // lease. Never use a remote wall clock or serialize Instant.
        let safe_lease = Duration::from_millis(known.lease_remaining_ms).checked_sub(WATCH_LEASE_SAFETY_MARGIN)?;
        let expires = started.checked_add(safe_lease)?;
        if expires <= now {
            return None;
        }
        deadline = Some(deadline.map_or(expires, |old: Instant| old.min(expires)));
        peers.insert(*owner, known.clone());
    }
    let proof = previous.cloned().unwrap_or_else(|| ScannerUsageWatchProof {
        version: WATCH_PROOF_VERSION,
        bucket: bucket.to_owned(),
        bucket_incarnation: incarnation,
        structure,
        peers,
    });
    proof.is_well_formed().then_some(LiveUsageWatch {
        start: proof,
        deadline: deadline?,
    })
}

async fn query_watch(
    store: &ECStore,
    bucket: &str,
    incarnation: Uuid,
    previous: Option<&ScannerUsageWatchProof>,
) -> Option<LiveUsageWatch> {
    if bucket.is_empty() || bucket.len() > 63 || incarnation.is_nil() {
        return None;
    }
    let distributed = store.setup_is_dist_erasure().await;
    let before = probe_scanner_activity(store, distributed).await.ok()?;
    if !crate::scanner::scanner_activity_allows_usage_publication(&before) {
        return None;
    }
    let instance_layout = watch_instance_layout_digest(&store.instance_endpoints()?, &before, distributed)?;
    let notifications = store.notification_system();
    let snapshots = if distributed {
        notifications.as_ref()?.scanner_dirty_usage_snapshots().await.ok()?
    } else {
        vec![]
    };
    let routes = owner_routes(store.id, &before, snapshots)?;
    let mut structure = Sha256::new();
    structure.update(b"scanner-usage-watch-instance-structure-v1");
    structure.update(watch_structure_digest(&before, &routes)?.0);
    structure.update(instance_layout.0);
    let structure = DataUsageScanPlanDigest(structure.finalize().into());
    if previous.is_some_and(|proof| proof.peers.keys().ne(routes.keys())) {
        return None;
    }
    // One bucket per owner keeps both entry and encoded-byte request bounds
    // below the protocol's 32 entries / 8 KiB budget, without truncation.
    let request_for = |owner: &Uuid| ScannerUsageWatchRequest {
        bucket: bucket.to_owned(),
        bucket_incarnation: incarnation,
        watch_id: previous.and_then(|proof| proof.peers.get(owner).map(|known| known.watch_id)),
        session_id: previous.and_then(|proof| proof.peers.get(owner).map(|known| known.session_id)),
        release: false,
    };
    let remote_requests = routes
        .iter()
        .filter_map(|(owner, host)| host.as_ref().map(|host| (host.clone(), *owner, vec![request_for(owner)])))
        .collect::<Vec<_>>();
    let started = Instant::now();
    let local = store.scanner_usage_watch(&[request_for(&store.id)]).await;
    let remote = if distributed {
        notifications.as_ref()?.scanner_usage_watches(remote_requests).await.ok()?
    } else {
        vec![]
    };
    let mut observations = BTreeMap::from([(store.id, local)]);
    for (host, values) in remote {
        let owner = routes
            .iter()
            .find_map(|(owner, route)| (route.as_ref() == Some(&host)).then_some(*owner))?;
        if observations.insert(owner, values).is_some() {
            return None;
        }
    }
    let after = probe_scanner_activity(store, distributed).await.ok()?;
    if crate::scanner::scanner_activity_guardrail_veto(&before, &after).is_some()
        || watch_instance_layout_digest(&store.instance_endpoints()?, &after, distributed)? != instance_layout
    {
        return None;
    }
    accept_watch_observations(bucket, incarnation, &routes, observations, previous, structure, started, Instant::now())
}

/// Legacy activity/notification accessors may still resolve process globals.
/// A C5 proof additionally requires every actual peer of this store instance
/// and binds its own full endpoint layout, so a globally self-consistent but
/// different cluster cannot stand in for a missing source.
fn watch_instance_layout_digest(
    endpoints: &EndpointServerPools,
    activity: &ScannerActivitySnapshot,
    distributed: bool,
) -> Option<DataUsageScanPlanDigest> {
    if endpoints.0.is_empty() {
        return None;
    }
    let slots = endpoints.peer_grid_host_slots_sorted();
    let mut expected = BTreeSet::new();
    let mut local = 0;
    for (_, grid_host, is_local) in &slots {
        if *is_local {
            local += 1;
        } else if !expected.insert(grid_host.as_ref()?.clone()) {
            return None;
        }
    }
    if (distributed && (local != 1 || expected.is_empty() || slots.len() > MAX_WATCH_OWNERS))
        || (!distributed && !expected.is_empty())
        || activity
            .keys()
            .filter(|host| host.as_str() != LOCAL_SCANNER_ACTIVITY_NODE)
            .ne(expected.iter())
    {
        return None;
    }
    let mut hash = Sha256::new();
    hash.update(b"scanner-usage-watch-instance-layout-v1");
    hash.update((endpoints.0.len() as u64).to_be_bytes());
    for (pool_index, pool) in endpoints.0.iter().enumerate() {
        hash.update((pool_index as u64).to_be_bytes());
        hash.update((pool.set_count as u64).to_be_bytes());
        hash.update((pool.drives_per_set as u64).to_be_bytes());
        let mut disks = pool.endpoints.as_ref().iter().collect::<Vec<_>>();
        disks.sort_unstable_by(|left, right| {
            (left.pool_idx, left.set_idx, left.disk_idx, left.url.as_str()).cmp(&(
                right.pool_idx,
                right.set_idx,
                right.disk_idx,
                right.url.as_str(),
            ))
        });
        hash.update((disks.len() as u64).to_be_bytes());
        for disk in disks {
            hash.update(disk.pool_idx.to_be_bytes());
            hash.update(disk.set_idx.to_be_bytes());
            hash.update(disk.disk_idx.to_be_bytes());
            hash.update((disk.url.as_str().len() as u64).to_be_bytes());
            hash.update(disk.url.as_str().as_bytes());
        }
    }
    Some(DataUsageScanPlanDigest(hash.finalize().into()))
}

fn watch_structure_digest(
    activity: &ScannerActivitySnapshot,
    routes: &BTreeMap<Uuid, Option<String>>,
) -> Option<DataUsageScanPlanDigest> {
    crate::scanner::scanner_activity_usage_watch_structure_digest(activity, routes).map(DataUsageScanPlanDigest)
}

pub(crate) fn bind_usage_watch_to_checkpoint(
    cache: &mut DataUsageCache,
    proof: Option<&ScannerUsageWatchProof>,
    requested_plan: DataUsageScanPlanDigest,
) {
    if cache
        .info
        .scan_progress
        .is_none_or(|progress| progress.started_plan == requested_plan)
    {
        cache.info.scan_usage_watch = proof.cloned();
    }
}

pub(super) async fn record_completed_usage_watch(
    completed: &Arc<Mutex<HashMap<DataUsageCacheSource, HashMap<String, ScannerUsageWatchProof>>>>,
    source: DataUsageCacheSource,
    bucket: &str,
    proof: Option<&ScannerUsageWatchProof>,
) {
    let mut completed = completed.lock().await;
    if let Some(proof) = proof {
        completed.entry(source).or_default().insert(bucket.to_owned(), proof.clone());
    } else if let Some(buckets) = completed.get_mut(&source) {
        buckets.remove(bucket);
    }
}

/// A complete EOF is still a useful observation when its mutation witness
/// fails. It is never a canonical LKG, and it leaves a fresh-sweep obligation.
pub(crate) fn invalidate_completed_usage_watch_cache(cache: &mut DataUsageCache, completed_plan: DataUsageScanPlanDigest) {
    let mut hash = Sha256::new();
    hash.update(b"scanner-usage-watch-unavailable-v1");
    hash.update(completed_plan.0);
    let unverified = DataUsageScanPlanDigest(hash.finalize().into());
    cache.info.scan_progress = Some(crate::DataUsageScanProgress {
        started_plan: completed_plan,
        requested_plan: unverified,
    });
    cache.record_completed_bucket_observation();
    cache.info.snapshot_complete = false;
    cache.info.lkg_snapshot_complete = false;
    cache.info.scan_bucket_value_origins.clear();
    cache.info.scan_plan_digest = None;
    cache.info.scan_usage_watch = None;
}

#[cfg(test)]
mod tests;
