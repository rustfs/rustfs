// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::{ADMIN_USAGE_INCARNATION_CONCURRENCY, ADMIN_USAGE_INCARNATION_TIMEOUT, BucketUsageInfo, ECStore};
use futures::StreamExt;
use std::{collections::HashMap, sync::Arc};
use tokio::time::{Instant, timeout_at};

/// Read only the caller-selected bucket identities. One deadline bounds the
/// entire batch, including concurrent reads waiting for metadata locks.
pub(crate) async fn current_admin_bucket_incarnations(
    store: Arc<ECStore>,
    buckets: Vec<String>,
    deadline: Instant,
) -> HashMap<String, String> {
    let mut current = HashMap::new();
    let mut reads = futures::stream::iter(buckets)
        .map(move |name| {
            let store = store.clone();
            async move {
                let id = store.bucket_incarnation_id_from_disk(&name).await;
                (name, id)
            }
        })
        .buffer_unordered(ADMIN_USAGE_INCARNATION_CONCURRENCY);
    while Instant::now() < deadline {
        let Ok(Some((name, id))) = timeout_at(deadline, reads.next()).await else {
            break;
        };
        if let Ok(id) = id
            && !id.is_nil()
        {
            current.insert(name, id.to_string());
        }
    }
    current
}

/// Verify both persisted scanner and single-node overlay values at the time of
/// an account-info read. Only names already selected by the IAM policy enter
/// metadata I/O; missing legacy identities and metadata timeouts remain unknown.
pub async fn verified_account_bucket_usage(
    store: Arc<ECStore>,
    mut measured: HashMap<String, BucketUsageInfo>,
    allowed_names: Vec<String>,
) -> HashMap<String, BucketUsageInfo> {
    let names = allowed_names
        .into_iter()
        .filter(|name| measured.get(name).is_some_and(|value| value.bucket_incarnation.is_some()))
        .collect();
    let current = current_admin_bucket_incarnations(store, names, Instant::now() + ADMIN_USAGE_INCARNATION_TIMEOUT).await;
    measured.retain(|name, value| {
        current
            .get(name)
            .is_some_and(|id| value.bucket_incarnation.as_ref() == Some(id))
    });
    measured
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
    use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};

    #[tokio::test]
    #[serial_test::serial]
    async fn account_inventory_preserves_only_permitted_current_incarnations_including_measured_zero() {
        let (_dirs, store) = isolated_store_over_temp_disks().await;
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let mut usage = HashMap::new();
        for (name, objects) in [("permitted", 0), ("private", 300), ("recreated", 900), ("legacy", 90)] {
            store.make_bucket(name, &MakeBucketOptions::default()).await.unwrap();
            let current = store.bucket_incarnation_id_from_disk(name).await.unwrap();
            let incarnation = match name {
                "recreated" => Some("11111111-1111-4111-8111-111111111111".to_string()),
                "legacy" => None,
                _ => Some(current.to_string()),
            };
            usage.insert(
                name.to_owned(),
                BucketUsageInfo {
                    objects_count: objects,
                    bucket_incarnation: incarnation,
                    ..Default::default()
                },
            );
        }
        let allowed = ["permitted", "recreated", "legacy"].map(String::from).to_vec();
        let values = verified_account_bucket_usage(store, usage, allowed).await;
        assert_eq!(values.len(), 1);
        assert_eq!(values["permitted"].objects_count, 0);
    }
}
