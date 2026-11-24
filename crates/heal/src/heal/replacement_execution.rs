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
    DiskError, DiskStore, HEALING_MARKER_PATH, RUSTFS_META_BUCKET,
    replacement_readiness::{ReplacementTargets, local_replacement_disks, probe_replacement_mounts},
    resume::ReplacementTargetIdentity,
    storage_api::{EcstoreConditionalFileUpdate, EcstoreDiskAPI, EcstoreDiskBytes, EcstoreReplacementExecutionLease},
};
use crate::{Error, Result};
use std::{future::Future, sync::Arc};

/// Owns every target of one local replacement executor. Metadata CAS locks
/// are acquired only after these execution leases, never in reverse order.
#[derive(Debug)]
pub struct ReplacementExecution {
    disks: Vec<DiskStore>,
    identities: Vec<ReplacementTargetIdentity>,
    _leases: Vec<Arc<EcstoreReplacementExecutionLease>>,
}

impl ReplacementExecution {
    pub(crate) async fn acquire(targets: &[String]) -> Result<Arc<Self>> {
        let targets = ReplacementTargets::open(targets)
            .await
            .ok_or_else(|| Error::ReplacementTargetNotReady("replacement target is unavailable".to_string()))?;
        let identities = targets
            .identities()
            .await
            .ok_or_else(|| Error::ReplacementTargetNotReady("target mount admission failed".to_string()))?;
        let mut leases = Vec::with_capacity(identities.len());
        // Take leases in endpoint order. Revalidate those same handles against
        // the current inventory so a reopened disk cannot supply their proof.
        for disk in &targets.disks {
            leases.push(disk.acquire_replacement_execution_lease().await?);
        }
        let local_disks = local_replacement_disks().await;
        if probe_replacement_mounts(&targets.disks, &local_disks).await.as_ref() != Some(&identities) {
            return Err(Error::ReplacementTargetNotReady(
                "target changed while acquiring execution leases".to_string(),
            ));
        }
        Ok(Arc::new(Self {
            disks: targets.disks,
            identities,
            _leases: leases,
        }))
    }

    pub(crate) fn identities(&self) -> &[ReplacementTargetIdentity] {
        &self.identities
    }

    pub(crate) async fn markers(&self) -> Result<Vec<Option<String>>> {
        if self.disks.len() != self.identities.len() {
            return Err(Error::ReplacementOwnershipConflict("healing marker target is unavailable".to_string()));
        }
        let mut markers = Vec::with_capacity(self.disks.len());
        for disk in &self.disks {
            let marker = match EcstoreDiskAPI::read_all(disk.as_ref(), RUSTFS_META_BUCKET, HEALING_MARKER_PATH).await {
                Ok(bytes) if bytes.len() <= 256 => Some(
                    String::from_utf8(bytes.to_vec())
                        .map_err(|_| Error::ReplacementOwnershipConflict("healing marker is not valid UTF-8".to_string()))?,
                ),
                Ok(_) => return Err(Error::ReplacementOwnershipConflict("healing marker exceeds its size limit".to_string())),
                Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => None,
                Err(error) => return Err(error.into()),
            };
            markers.push(marker);
        }
        Ok(markers)
    }

    pub(crate) async fn acquire_markers(&self, marker: &str) -> Result<()> {
        if self.markers().await?.iter().flatten().any(|actual| actual != marker) {
            return Err(Error::ReplacementOwnershipConflict("healing marker has an unknown owner".to_string()));
        }
        super::apply_healing_markers_to_targets(self.disks.clone(), Some(marker), None, false).await
    }

    /// A prepared handoff owns partial publication. Keep it replayable instead
    /// of deleting markers already transferred to the fixed successor.
    pub(crate) async fn transfer_markers(&self, expected: &[Option<String>], marker: &str) -> Result<()> {
        if expected.len() != self.disks.len() || self.disks.len() != self.identities.len() {
            return Err(Error::ReplacementOwnershipConflict("handoff target count changed".to_string()));
        }
        let new_marker = EcstoreDiskBytes::copy_from_slice(marker.as_bytes());
        for (disk, expected) in self.disks.iter().zip(expected) {
            let expected = expected
                .as_ref()
                .map(|value| EcstoreDiskBytes::copy_from_slice(value.as_bytes()));
            let result = EcstoreDiskAPI::compare_and_update_file(
                disk.as_ref(),
                RUSTFS_META_BUCKET,
                HEALING_MARKER_PATH,
                expected,
                Some(new_marker.clone()),
            )
            .await?;
            if matches!(result, EcstoreConditionalFileUpdate::Updated) {
                continue;
            }
            let idempotent = EcstoreDiskAPI::compare_and_update_file(
                disk.as_ref(),
                RUSTFS_META_BUCKET,
                HEALING_MARKER_PATH,
                Some(new_marker.clone()),
                Some(new_marker.clone()),
            )
            .await?;
            if !matches!(idempotent, EcstoreConditionalFileUpdate::Updated) {
                return Err(Error::ReplacementOwnershipConflict(
                    "healing marker ownership changed during handoff".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// A dropped page waiter must not release the lease of a mutation that is
    /// still executing. The owned worker keeps the lease through its I/O.
    pub(crate) async fn run<T: Send + 'static>(self: Arc<Self>, work: impl Future<Output = T> + Send + 'static) -> Result<T> {
        self.run_with_owner(None, work).await
    }

    pub(super) async fn run_owned<T: Send + 'static>(
        self: Arc<Self>,
        parent: &super::manager::HealWork,
        work: impl Future<Output = T> + Send + 'static,
    ) -> Result<T> {
        self.run_with_owner(Some(parent), work).await
    }

    async fn run_with_owner<T: Send + 'static>(
        self: Arc<Self>,
        parent: Option<&super::manager::HealWork>,
        work: impl Future<Output = T> + Send + 'static,
    ) -> Result<T> {
        super::manager::spawn_heal_work(parent, move |_parent| async move {
            let _execution = self;
            work.await
        })
        .await
        .map_err(|error| Error::other(format!("replacement worker failed: {error}")))
    }

    #[cfg(test)]
    pub(crate) fn test_disks(&self) -> &[DiskStore] {
        &self.disks
    }

    #[cfg(test)]
    pub(crate) fn for_test(disks: Vec<DiskStore>, identities: Vec<ReplacementTargetIdentity>) -> Arc<Self> {
        Arc::new(Self {
            disks,
            identities,
            _leases: Vec::new(),
        })
    }
}
