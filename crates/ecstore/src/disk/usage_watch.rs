// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Explicit instance binding at the physical disk boundary. Lock order is this
//! binding mutex, then the registry mutex; registry code never locks a disk.

use crate::runtime::usage_watch::{UsageWatchMutationGuard, UsageWatchRegistry};
use std::sync::{Arc, Mutex, Weak};

/// Existing-version replication status writes change neither size nor version
/// membership. Replication aggregates keep their existing maintenance cadence.
pub(crate) fn status_only_metadata_update(fi: &rustfs_filemeta::FileInfo, opts: &super::UpdateMetadataOpts) -> bool {
    use rustfs_utils::http::*;
    !opts.replace_user_metadata
        && opts.transition_reconcile.is_none()
        && !fi.metadata.is_empty()
        && fi.metadata.keys().all(|key| {
            strip_internal_prefix_preserving_case(key).is_some_and(|suffix| {
                [
                    SUFFIX_REPLICA_STATUS,
                    SUFFIX_REPLICA_TIMESTAMP,
                    SUFFIX_REPLICATION_STATUS,
                    SUFFIX_REPLICATION_TIMESTAMP,
                    SUFFIX_REPLICATION_GENERATION,
                    SUFFIX_REPLICATION_RESET,
                    SUFFIX_PURGESTATUS,
                ]
                .iter()
                .any(|known| suffix.eq_ignore_ascii_case(known))
            })
        })
}

#[derive(Default)]
struct BindingState {
    registry: Option<Weak<UsageWatchRegistry>>,
    unbound_inflight: u64,
    prebinding_guard: Option<UsageWatchMutationGuard>,
    conflicted: bool,
}

#[derive(Default)]
pub(crate) struct DiskUsageWatchBinding {
    state: Mutex<BindingState>,
}

struct UnboundGuard(Arc<DiskUsageWatchBinding>);

impl Drop for UnboundGuard {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.unbound_inflight != 0 {
            state.unbound_inflight -= 1;
        }
        if state.unbound_inflight == 0 {
            state.prebinding_guard = None;
        }
    }
}

impl DiskUsageWatchBinding {
    pub(crate) fn mark_uncovered_user_stream(&self, bucket: &str) {
        if crate::bucket::utils::is_meta_bucketname(bucket) {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.conflicted = true;
        if let Some(registry) = state.registry.as_ref().and_then(Weak::upgrade) {
            registry.mark_coverage_unavailable();
        }
    }

    pub(crate) fn bind(&self, registry: &Arc<UsageWatchRegistry>) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = state.registry.as_ref().and_then(Weak::upgrade) {
            if !Arc::ptr_eq(&existing, registry) {
                state.conflicted = true;
                // A query may already have checked this binding before
                // awaiting disk metadata. Never allow it to reinstall a
                // proof after this permanently ambiguous ownership change.
                existing.mark_coverage_unavailable();
                registry.mark_coverage_unavailable();
            }
            return;
        }
        // A newly registered disk can have pre-registration physical tails.
        // Keep the whole registry Unknown until those exact guards drain.
        if state.unbound_inflight != 0 {
            state.prebinding_guard = Some(registry.begin(None));
        }
        if state.conflicted {
            registry.mark_coverage_unavailable();
        }
        state.registry = Some(Arc::downgrade(registry));
    }

    pub(crate) fn matches(&self, registry: &Arc<UsageWatchRegistry>) -> bool {
        let state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        !state.conflicted
            && state
                .registry
                .as_ref()
                .and_then(Weak::upgrade)
                .is_some_and(|bound| Arc::ptr_eq(&bound, registry))
    }

    pub(crate) fn owner(self: &Arc<Self>, bucket: &str, parent: Option<Arc<dyn Send + Sync>>) -> Option<Arc<dyn Send + Sync>> {
        if crate::bucket::utils::is_meta_bucketname(bucket) {
            return parent;
        }
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let guard: Arc<dyn Send + Sync> = if let Some(registry) = state.registry.as_ref().and_then(Weak::upgrade) {
            Arc::new(registry.begin((!state.conflicted).then_some(bucket)))
        } else {
            match state.unbound_inflight.checked_add(1) {
                Some(count) => state.unbound_inflight = count,
                None => state.conflicted = true,
            }
            Arc::new(UnboundGuard(Arc::clone(self)))
        };
        Some(match parent {
            Some(parent) => Arc::new((parent, guard)),
            None => guard,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::usage_watch::ScannerUsageWatchRequest;
    use uuid::Uuid;

    #[test]
    fn binding_preserves_mutations_that_started_before_registration() {
        let binding = Arc::new(DiskUsageWatchBinding::default());
        let registry = Arc::new(UsageWatchRegistry::default());
        let tail = binding.owner("bucket", None);
        binding.bind(&registry);
        assert!(binding.matches(&registry));
        let request = ScannerUsageWatchRequest {
            bucket: "bucket".into(),
            bucket_incarnation: Uuid::from_u128(2),
            watch_id: None,
            release: false,
            session_id: None,
        };
        assert_eq!(registry.observe(Uuid::from_u128(1), &request), None);
        drop(tail);
        assert_eq!(
            registry
                .observe(Uuid::from_u128(1), &request)
                .expect("drained binding")
                .inflight,
            0
        );
    }

    #[test]
    fn composite_owner_and_instance_conflicts_fail_closed() {
        let binding = Arc::new(DiskUsageWatchBinding::default());
        let registry = Arc::new(UsageWatchRegistry::default());
        binding.bind(&registry);
        let request = ScannerUsageWatchRequest {
            bucket: "bucket".into(),
            bucket_incarnation: Uuid::from_u128(2),
            watch_id: None,
            release: false,
            session_id: None,
        };
        registry.observe(Uuid::from_u128(1), &request).expect("initial watch");
        let owner = binding.owner("bucket", None).expect("physical owner");
        let nested = binding.owner("bucket", Some(owner));
        assert_eq!(
            registry
                .observe(Uuid::from_u128(1), &request)
                .expect("both operations active")
                .inflight,
            2
        );
        drop(nested);
        assert_eq!(
            registry
                .observe(Uuid::from_u128(1), &request)
                .expect("drained owners")
                .inflight,
            0
        );
        let other = Arc::new(UsageWatchRegistry::default());
        binding.bind(&other);
        assert!(!binding.matches(&registry));
        assert!(!binding.matches(&other));
        assert_eq!(registry.observe(Uuid::from_u128(1), &request), None);
        assert_eq!(other.observe(Uuid::from_u128(1), &request), None);
    }
}
