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

//! Set-scoped listing (`SetDisks::walk`) and the borrow-based
//! prefix-maintenance service unit (`SetDisks::delete_all`).
//!
//! Paginated listings are an `ECStore` concern: they merge every set of every
//! pool, so `SetDisks` deliberately does not implement the `ListOperations`
//! storage-api contract.

use super::super::ctx::SetDisksCtx;
use super::super::{
    Arc, CancellationToken, DeleteOptions, DiskError, DiskStore, OBJECT_OP_IGNORED_ERRS, ObjectInfoOrErr, Result, Sender,
    SetDisks, WalkOptions, debug, join_all, reduce_write_quorum_errs,
};
use crate::disk::DiskAPI;

impl SetDisks {
    #[tracing::instrument(skip(self))]
    pub async fn delete_all(&self, bucket: &str, prefix: &str) -> Result<()> {
        let (result, disks) = ListOperations::new(self.ctx())
            .delete_all_observed(bucket, prefix, None)
            .await;
        self.record_capacity_scope_if_needed(None, &disks);
        result
    }

    pub(crate) async fn delete_all_with_quorum(&self, bucket: &str, prefix: &str, write_quorum: usize) -> Result<()> {
        let (result, disks) = ListOperations::new(self.ctx())
            .delete_all_observed(bucket, prefix, Some(write_quorum))
            .await;
        self.record_capacity_scope_if_needed(None, &disks);
        result
    }
}

/// List/prefix maintenance operations, borrowing the `SetDisks` core state
/// through [`SetDisksCtx`].
///
/// First validation of the borrow pattern for the SetDisks split (#816). The
/// behavior here is byte-for-byte identical to the previous inherent
/// `SetDisks::delete_all`; only state access moves from `self` to the borrow
/// handle.
pub(crate) struct ListOperations<'a> {
    ctx: SetDisksCtx<'a>,
}

impl<'a> ListOperations<'a> {
    pub(crate) fn new(ctx: SetDisksCtx<'a>) -> Self {
        Self { ctx }
    }

    pub(crate) async fn delete_all_observed(
        &self,
        bucket: &str,
        prefix: &str,
        write_quorum: Option<usize>,
    ) -> (Result<()>, Vec<Option<DiskStore>>) {
        let disks = self.ctx.disks().read().await.clone();
        let result = self.delete_all_inner(bucket, prefix, write_quorum, disks.clone()).await;
        (result, disks)
    }

    async fn delete_all_inner(
        &self,
        bucket: &str,
        prefix: &str,
        write_quorum: Option<usize>,
        disks: Vec<Option<DiskStore>>,
    ) -> Result<()> {
        let mut futures = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.delete(
                        bucket,
                        prefix,
                        DeleteOptions {
                            recursive: true,
                            ..Default::default()
                        },
                    )
                    .await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(_) => {
                    errors.push(None);
                }
                Err(DiskError::FileNotFound | DiskError::PathNotFound | DiskError::VolumeNotFound) => {
                    errors.push(None);
                }
                Err(e) => {
                    errors.push(Some(e));
                }
            }
        }

        let failed = errors.iter().filter(|err| err.is_some()).count();
        if failed > 0 {
            debug!(
                bucket = %bucket,
                prefix = %prefix,
                failed,
                total = errors.len(),
                errors = ?errors,
                "delete_all completed with disk errors"
            );
        }

        if let Some(write_quorum) = write_quorum
            && let Some(err) = reduce_write_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, write_quorum)
        {
            return Err(err.into());
        }

        Ok(())
    }
}

impl SetDisks {
    /// Streams every object under `prefix` of this erasure set into `result`.
    ///
    /// Paginated listings (`ListObjectsV2`, `ListObjectVersions`) exist only on
    /// `ECStore`, which merges all sets of all pools; a single set cannot answer
    /// them on its own. Set-scoped callers (prefix deletes, tier reference
    /// proofs, pool-local maintenance) walk the set instead.
    pub async fn walk(
        self: Arc<Self>,
        rx: CancellationToken,
        bucket: &str,
        prefix: &str,
        result: Sender<ObjectInfoOrErr>,
        opts: WalkOptions,
    ) -> Result<()> {
        self.walk_internal(rx, bucket, prefix, result, opts).await
    }
}
