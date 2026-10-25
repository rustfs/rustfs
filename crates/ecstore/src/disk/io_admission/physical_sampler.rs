// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Physical ownership is discovered and renewed by the single sampling worker,
//! never by an object operation or a native I/O job.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use rustfs_concurrency::adaptive::permit_set::{AdaptivePermitSetOwner, NodeResourcePermit};
use rustfs_concurrency::adaptive::{AdaptiveAdmission, FeedbackController, FeedbackObservation, NodeAdmission, ResourcePressure};

use super::physical_identity::{PhysicalDeviceIdentity, resolve_physical_devices};
use super::physical_owners::PhysicalOwnerRegistry;
use crate::disk::{Disk, local::LocalDisk};
use crate::store::ECStore;

const LEASE: Duration = Duration::from_secs(3);
const MAXIMUM: NonZeroU16 = NonZeroU16::new(256).expect("fixed physical admission bound");
const MAX_LOGICAL_DISKS: usize = 4096;

/// Shared by clones of one local attachment. Reconnects get a new binding;
/// aliases still resolve to the same physical owners in the sampling registry.
#[derive(Default)]
pub struct PhysicalDiskBinding(ArcSwapOption<PhysicalDiskPlan>);

impl std::fmt::Debug for PhysicalDiskBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalDiskBinding")
            .field("ready", &self.plan().is_some())
            .finish()
    }
}

impl PhysicalDiskBinding {
    /// Explicit synthetic ownership for cross-crate admission regressions.
    #[cfg(any(test, feature = "test-util"))]
    pub fn set_test_capacity(&self, gates: Vec<Arc<AdaptiveAdmission>>) -> std::io::Result<()> {
        let owner = AdaptivePermitSetOwner::new(MAXIMUM, gates)?;
        self.invalidate();
        let plan = Arc::new(PhysicalDiskPlan {
            identities: Vec::new(),
            owner,
            origin: Instant::now(),
            valid_until_millis: AtomicU64::new(0),
        });
        plan.renew();
        self.0.store(Some(plan));
        Ok(())
    }

    /// Missing or expired discovery is unavailable, never a node-only fallback.
    pub fn plan(&self) -> Option<Arc<PhysicalDiskPlan>> {
        self.0.load_full().filter(|plan| plan.is_current())
    }

    fn invalidate(&self) {
        if let Some(plan) = self.0.swap(None) {
            plan.valid_until_millis.store(0, Ordering::Release);
        }
    }
}

/// Preallocated admission for the actual leaves of one local disk. The binding
/// lease is independent of each physical gate: a healthy alias cannot renew a
/// removed attachment's topology.
pub struct PhysicalDiskPlan {
    identities: Vec<PhysicalDeviceIdentity>,
    owner: Arc<AdaptivePermitSetOwner>,
    origin: Instant,
    valid_until_millis: AtomicU64,
}

impl PhysicalDiskPlan {
    pub fn is_current(&self) -> bool {
        self.origin.elapsed().as_millis() < u128::from(self.valid_until_millis.load(Ordering::Acquire))
    }

    fn renew(&self) {
        let deadline = self.origin.elapsed().saturating_add(LEASE).as_millis();
        self.valid_until_millis
            .store(u64::try_from(deadline).unwrap_or(u64::MAX), Ordering::Release);
    }

    /// Never wait under a storage lock. A refresh during acquisition rejects
    /// the old plan before its future can issue I/O.
    pub fn try_acquire(&self, node: &Arc<NodeAdmission>) -> Option<NodeResourcePermit> {
        if !self.is_current() {
            return None;
        }
        let permit = self.owner.try_acquire_with_node(node)?;
        self.is_current().then_some(permit)
    }

    /// Keep FIFO position while discovery is current. The caller owns scan
    /// cancellation/deadlines outside cache locks; expiry returns a deferral.
    pub async fn acquire(&self, node: &Arc<NodeAdmission>) -> Option<NodeResourcePermit> {
        if !self.is_current() {
            return None;
        }
        let acquisition = self.owner.acquire_with_node(node);
        tokio::pin!(acquisition);
        loop {
            tokio::select! {
                result = &mut acquisition => return self.is_current().then_some(result.ok()?),
                _ = tokio::time::sleep(Duration::from_millis(250)) => {
                    if !self.is_current() {
                        return None;
                    }
                }
            }
        }
    }
}

struct Target {
    disk: Arc<LocalDisk>,
    binding: Arc<PhysicalDiskBinding>,
}

/// Collected handles contain no storage locks. Move this snapshot into the
/// existing blocking sampler; only `publish` may renew admission leases.
pub struct PhysicalDiskSample {
    targets: Vec<Target>,
    resolved: Vec<Resolved>,
    devices: BTreeMap<PhysicalDeviceIdentity, Option<Observation>>,
    finished: Option<Instant>,
}

struct Resolved {
    binding: Arc<PhysicalDiskBinding>,
    identities: Option<Vec<PhysicalDeviceIdentity>>,
    began: Instant,
    ended: Instant,
}

#[derive(Clone, Copy)]
struct Observation {
    began: Instant,
    ended: Instant,
    ios: u64,
    ticks: u64,
    queued_ticks: u64,
    in_flight: u64,
    maximum: u32,
}

struct Device {
    gate: Arc<AdaptiveAdmission>,
    controller: FeedbackController,
    previous: Option<Observation>,
}

/// Node-local state, owned exclusively by the existing feedback loop.
pub struct PhysicalDiskSampler {
    registry: PhysicalOwnerRegistry,
    devices: BTreeMap<PhysicalDeviceIdentity, Device>,
    bindings: Vec<Arc<PhysicalDiskBinding>>,
    probe: usize,
}

impl Default for PhysicalDiskSampler {
    fn default() -> Self {
        Self {
            registry: PhysicalOwnerRegistry::new(MAXIMUM),
            devices: BTreeMap::new(),
            bindings: Vec::new(),
            probe: 0,
        }
    }
}

impl Drop for PhysicalDiskSampler {
    fn drop(&mut self) {
        for binding in &self.bindings {
            binding.invalidate();
        }
        for device in self.devices.values() {
            device.gate.set_sampled_limit(0, Duration::ZERO);
        }
    }
}

impl PhysicalDiskSample {
    /// Snapshot current attachments before blocking sysfs reads. A busy set
    /// cannot hold the sampler indefinitely or renew stale capacity.
    pub fn collect(store: &ECStore) -> Self {
        let mut targets = Vec::new();
        let mut seen = BTreeSet::new();
        for pool in &store.pools {
            for set in &pool.disk_set {
                let Ok(disks) = set.disks.try_read() else { continue };
                for disk in disks.iter().flatten() {
                    let Disk::Local(local) = disk.as_ref() else { continue };
                    let binding = local.physical_admission();
                    if targets.len() < MAX_LOGICAL_DISKS && seen.insert(Arc::as_ptr(&binding)) {
                        targets.push(Target {
                            disk: local.get_disk(),
                            binding,
                        });
                    }
                }
            }
        }
        Self {
            targets,
            resolved: Vec::new(),
            devices: BTreeMap::new(),
            finished: None,
        }
    }

    /// Blocking read only. Cancellation may abandon this worker, so publication
    /// is deliberately left to the live async owner after joining it.
    pub fn read(mut self) -> Self {
        for target in std::mem::take(&mut self.targets) {
            let began = Instant::now();
            let identities = (|| {
                if !target.disk.has_replacement_mount_lease() {
                    return None;
                }
                let ids = resolve_physical_devices(Path::new("/sys"), target.disk.major, target.disk.minor).ok()?;
                for id in &ids {
                    if self.devices.len() >= usize::from(MAXIMUM.get()) && !self.devices.contains_key(id) {
                        return None;
                    }
                    self.devices.entry(*id).or_insert_with(|| read_device(*id));
                }
                let after = resolve_physical_devices(Path::new("/sys"), target.disk.major, target.disk.minor).ok()?;
                (ids == after && target.disk.has_replacement_mount_lease()).then_some(ids)
            })();
            self.resolved.push(Resolved {
                binding: target.binding,
                identities,
                began,
                ended: Instant::now(),
            });
        }
        self.finished = Some(Instant::now());
        self
    }
}

impl PhysicalDiskSampler {
    /// One round-robin probe slot includes the node controller, at index zero.
    /// Controllers without that slot may still cut capacity on every window.
    pub fn node_may_probe(&self) -> bool {
        self.probe == 0
    }

    pub fn publish(&mut self, sample: PhysicalDiskSample, pressure: ResourcePressure) {
        let now = Instant::now();
        let fresh = sample
            .finished
            .is_some_and(|at| now.saturating_duration_since(at) <= Duration::from_secs(1));
        let mut live = BTreeSet::new();
        for resolved in &sample.resolved {
            let binding = &resolved.binding;
            let Some(identities) = resolved.identities.as_ref().filter(|_| {
                fresh
                    && now
                        .checked_duration_since(resolved.began)
                        .is_some_and(|age| age <= Duration::from_secs(1))
                    && resolved
                        .ended
                        .checked_duration_since(resolved.began)
                        .is_some_and(|width| width <= Duration::from_millis(20))
            }) else {
                binding.invalidate();
                continue;
            };
            let Ok(owners) = self.registry.bind(identities) else {
                binding.invalidate();
                continue;
            };
            for (identity, owner) in identities.iter().zip(&owners) {
                live.insert(*identity);
                self.devices.entry(*identity).or_insert_with(|| Device {
                    gate: owner.gate().clone(),
                    controller: FeedbackController::new(),
                    previous: None,
                });
            }
            let previous = binding.0.load_full();
            if let Some(plan) = previous.filter(|plan| plan.identities == *identities) {
                plan.renew();
            } else {
                binding.invalidate();
                let Ok(owner) = AdaptivePermitSetOwner::new(MAXIMUM, owners.iter().map(|owner| owner.gate().clone()).collect())
                else {
                    continue;
                };
                let plan = Arc::new(PhysicalDiskPlan {
                    identities: identities.clone(),
                    owner,
                    origin: now,
                    valid_until_millis: AtomicU64::new(0),
                });
                plan.renew();
                binding.0.store(Some(plan));
            }
        }
        let attached = sample
            .resolved
            .iter()
            .map(|resolved| Arc::as_ptr(&resolved.binding))
            .collect::<BTreeSet<_>>();
        for binding in &self.bindings {
            if !attached.contains(&Arc::as_ptr(binding)) {
                binding.invalidate();
            }
        }
        self.bindings = sample.resolved.into_iter().map(|resolved| resolved.binding).collect();
        self.devices.retain(|id, device| {
            if live.contains(id) {
                true
            } else {
                device.gate.set_sampled_limit(0, Duration::ZERO);
                false
            }
        });
        for (index, (identity, device)) in self.devices.iter_mut().enumerate() {
            let observation = sample.devices.get(identity).copied().flatten().filter(|sample| {
                fresh
                    && now.saturating_duration_since(sample.began) <= Duration::from_secs(1)
                    && sample.ended.duration_since(sample.began) <= Duration::from_millis(20)
            });
            let prior = std::mem::replace(&mut device.previous, observation);
            let feedback = observation.map(|sample| FeedbackObservation {
                generation: identity.generation,
                max_concurrency: sample.maximum,
                pressure: combine_pressure(pressure, disk_pressure(sample, prior)),
                completed_units: device.gate.completed(),
            });
            device
                .controller
                .update_with_probe(&device.gate, feedback, self.probe == index + 1);
        }
        self.probe = (self.probe + 1) % (self.devices.len() + 1);
    }
}

fn read_device(id: PhysicalDeviceIdentity) -> Option<Observation> {
    let began = Instant::now();
    let ids = resolve_physical_devices(Path::new("/sys"), u64::from(id.major), u64::from(id.minor)).ok()?;
    if ids != [id] {
        return None;
    }
    let stats = rustfs_utils::os::get_drive_stats(id.major, id.minor).ok()?;
    let path = std::path::PathBuf::from(format!("/sys/dev/block/{}:{}/queue", id.major, id.minor));
    let rotational = super::physical_identity::attribute(&path.join("rotational")).ok();
    let maximum = match rotational.as_deref() {
        Some("0") => super::physical_identity::attribute(&path.join("nr_requests"))
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(1)
            .min(u32::from(MAXIMUM.get())),
        _ => 1,
    };
    if resolve_physical_devices(Path::new("/sys"), u64::from(id.major), u64::from(id.minor)).ok()? != [id] {
        return None;
    }
    Some(Observation {
        began,
        ended: Instant::now(),
        ios: stats
            .read_ios
            .checked_add(stats.write_ios)?
            .checked_add(stats.discard_ios)?
            .checked_add(stats.flush_ios)?,
        ticks: stats.total_ticks,
        queued_ticks: stats.req_ticks,
        in_flight: stats.current_ios,
        maximum,
    })
}

fn combine_pressure(a: ResourcePressure, b: ResourcePressure) -> ResourcePressure {
    match (a, b) {
        (ResourcePressure::Busy, _) | (_, ResourcePressure::Busy) => ResourcePressure::Busy,
        (ResourcePressure::Stable, ResourcePressure::Stable) => ResourcePressure::Stable,
        _ => ResourcePressure::Unknown,
    }
}

fn disk_pressure(current: Observation, previous: Option<Observation>) -> ResourcePressure {
    let Some(previous) = previous else { return ResourcePressure::Unknown };
    let Some(minimum) = current.began.checked_duration_since(previous.ended) else {
        return ResourcePressure::Unknown;
    };
    let Some(maximum) = current.ended.checked_duration_since(previous.began) else {
        return ResourcePressure::Unknown;
    };
    if minimum < Duration::from_millis(500) || maximum > Duration::from_secs(2) {
        return ResourcePressure::Unknown;
    }
    let Some((ticks, queued)) = current
        .ticks
        .checked_sub(previous.ticks)
        .zip(current.queued_ticks.checked_sub(previous.queued_ticks))
    else {
        return ResourcePressure::Unknown;
    };
    if current.ios < previous.ios {
        return ResourcePressure::Unknown;
    }
    if u128::from(ticks) * 100 > maximum.as_millis() * 90 || u128::from(queued) > maximum.as_millis() {
        ResourcePressure::Busy
    } else if current.in_flight == 0
        && u128::from(ticks) * 100 <= minimum.as_millis() * 50
        && u128::from(queued) * 100 <= minimum.as_millis() * 50
    {
        ResourcePressure::Stable
    } else {
        ResourcePressure::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(generation: u64) -> PhysicalDeviceIdentity {
        PhysicalDeviceIdentity {
            major: 259,
            minor: 0,
            generation,
        }
    }

    fn observation(at: Instant) -> Observation {
        Observation {
            began: at,
            ended: at,
            ios: 10,
            ticks: 0,
            queued_ticks: 0,
            in_flight: 0,
            maximum: 4,
        }
    }

    fn sample(bindings: &[Arc<PhysicalDiskBinding>], id: PhysicalDeviceIdentity) -> PhysicalDiskSample {
        let now = Instant::now();
        PhysicalDiskSample {
            targets: Vec::new(),
            resolved: bindings
                .iter()
                .map(|binding| Resolved {
                    binding: binding.clone(),
                    identities: Some(vec![id]),
                    began: now,
                    ended: now,
                })
                .collect(),
            devices: [(id, Some(observation(now)))].into(),
            finished: Some(now),
        }
    }

    fn node() -> Arc<NodeAdmission> {
        let node = NodeAdmission::new(MAXIMUM);
        node.update(
            &mut FeedbackController::new(),
            Some(FeedbackObservation {
                generation: 1,
                max_concurrency: 4,
                pressure: ResourcePressure::Unknown,
                completed_units: None,
            }),
        );
        node
    }

    #[test]
    fn aliases_share_one_gate_and_refresh_keeps_the_admitted_owner() {
        let a = Arc::new(PhysicalDiskBinding::default());
        let b = Arc::new(PhysicalDiskBinding::default());
        let mut sampler = PhysicalDiskSampler::default();
        sampler.publish(sample(&[a.clone(), b.clone()], identity(1)), ResourcePressure::Stable);
        let plan = a.plan().expect("discovered local disk");
        let node = node();
        let permit = plan.try_acquire(&node).expect("first alias admitted");
        let native = permit.resources().clone();
        permit.complete();
        assert_eq!(node.snapshot().active, 0);
        assert_eq!(sampler.devices[&identity(1)].gate.completed(), Some(1));
        assert!(
            b.plan().expect("alias plan").try_acquire(&node).is_none(),
            "native tail owns the shared disk unit"
        );
        sampler.publish(sample(&[a.clone(), b.clone()], identity(1)), ResourcePressure::Stable);
        assert!(Arc::ptr_eq(&plan, &a.plan().expect("same topology")));
        drop(native);
        drop(
            b.plan()
                .expect("alias")
                .try_acquire(&node)
                .expect("native release restores alias capacity"),
        );
        assert_eq!(sampler.devices[&identity(1)].gate.completed(), Some(1), "plain drop is not progress");
    }

    #[test]
    fn stale_attachment_cannot_be_renewed_by_a_healthy_alias() {
        let a = Arc::new(PhysicalDiskBinding::default());
        let b = Arc::new(PhysicalDiskBinding::default());
        let mut sampler = PhysicalDiskSampler::default();
        sampler.publish(sample(&[a.clone(), b.clone()], identity(1)), ResourcePressure::Stable);
        let stale = a.plan().expect("initial plan");
        let mut next = sample(&[a.clone(), b.clone()], identity(1));
        next.resolved[0].began -= Duration::from_secs(2);
        next.resolved[0].ended -= Duration::from_secs(2);
        sampler.publish(next, ResourcePressure::Stable);
        assert!(a.plan().is_none());
        assert!(!stale.is_current());
        let node = node();
        assert!(stale.try_acquire(&node).is_none());
        assert_eq!(node.snapshot().active, 0);
        assert!(b.plan().expect("fresh alias").try_acquire(&node).is_some());
    }

    #[test]
    fn replacement_generation_never_receives_old_native_refunds() {
        let binding = Arc::new(PhysicalDiskBinding::default());
        let mut sampler = PhysicalDiskSampler::default();
        sampler.publish(sample(std::slice::from_ref(&binding), identity(1)), ResourcePressure::Stable);
        let node = node();
        let permit = binding
            .plan()
            .expect("first incarnation")
            .try_acquire(&node)
            .expect("first work");
        let old_gate = sampler.devices[&identity(1)].gate.clone();
        let native = permit.resources().clone();
        drop(permit);
        sampler.publish(sample(std::slice::from_ref(&binding), identity(2)), ResourcePressure::Stable);
        let new_gate = sampler.devices[&identity(2)].gate.clone();
        assert!(!Arc::ptr_eq(&old_gate, &new_gate));
        let new = binding
            .plan()
            .expect("replacement")
            .try_acquire(&node)
            .expect("new generation work");
        drop(native);
        assert_eq!(old_gate.snapshot().active, 0);
        assert_eq!(old_gate.snapshot().limit, 0);
        assert_eq!(new_gate.snapshot().active, 1);
        drop(new);
    }

    #[test]
    fn sampler_shutdown_closes_plans_without_releasing_native_work() {
        let binding = Arc::new(PhysicalDiskBinding::default());
        let mut sampler = PhysicalDiskSampler::default();
        sampler.publish(sample(std::slice::from_ref(&binding), identity(1)), ResourcePressure::Stable);
        let plan = binding.plan().expect("plan");
        let node = node();
        let permit = plan.try_acquire(&node).expect("work");
        let native = permit.resources().clone();
        let gate = sampler.devices[&identity(1)].gate.clone();
        drop(permit);
        drop(sampler);
        assert!(!plan.is_current());
        assert!(binding.plan().is_none());
        assert_eq!(gate.snapshot().limit, 0);
        assert_eq!(gate.snapshot().active, 1);
        drop(native);
        assert_eq!(gate.snapshot().active, 0);
    }

    #[test]
    fn missing_stats_close_capacity_and_late_publication_invalidates_topology() {
        let binding = Arc::new(PhysicalDiskBinding::default());
        let mut sampler = PhysicalDiskSampler::default();
        let mut missing = sample(std::slice::from_ref(&binding), identity(1));
        missing.devices.insert(identity(1), None);
        sampler.publish(missing, ResourcePressure::Stable);
        let plan = binding.plan().expect("identity still known");
        assert!(plan.try_acquire(&node()).is_none(), "missing stats are not idle");
        let mut delayed = sample(std::slice::from_ref(&binding), identity(1));
        delayed.finished = Some(Instant::now() - Duration::from_secs(2));
        sampler.publish(delayed, ResourcePressure::Stable);
        assert!(!plan.is_current());
        assert!(binding.plan().is_none());
        assert!(sampler.devices.is_empty());
    }

    #[test]
    fn node_and_physical_probe_slots_rotate() {
        let binding = Arc::new(PhysicalDiskBinding::default());
        let mut sampler = PhysicalDiskSampler::default();
        assert!(sampler.node_may_probe());
        sampler.publish(sample(std::slice::from_ref(&binding), identity(1)), ResourcePressure::Stable);
        assert!(!sampler.node_may_probe());
        sampler.publish(sample(std::slice::from_ref(&binding), identity(1)), ResourcePressure::Stable);
        assert!(sampler.node_may_probe());
    }

    #[test]
    fn disk_windows_reject_resets_gaps_and_outstanding_work() {
        let now = Instant::now();
        let before = observation(now - Duration::from_secs(1));
        let mut after = observation(now);
        assert_eq!(disk_pressure(after, Some(before)), ResourcePressure::Stable);
        after.ios = 0;
        assert_eq!(disk_pressure(after, Some(before)), ResourcePressure::Unknown);
        after.ios = 11;
        after.in_flight = 1;
        assert_eq!(disk_pressure(after, Some(before)), ResourcePressure::Unknown);
        after.ticks = 950;
        assert_eq!(disk_pressure(after, Some(before)), ResourcePressure::Busy);
        after = observation(now + Duration::from_secs(3));
        assert_eq!(disk_pressure(after, Some(before)), ResourcePressure::Unknown);
        assert_eq!(
            combine_pressure(ResourcePressure::Unknown, ResourcePressure::Stable),
            ResourcePressure::Unknown
        );
    }
}
