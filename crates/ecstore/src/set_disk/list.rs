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

use super::ctx::SetDisksCtx;
use super::*;

impl SetDisks {
    #[tracing::instrument(skip(self))]
    pub async fn delete_all(&self, bucket: &str, prefix: &str) -> Result<()> {
        ListOperations::new(self.ctx()).delete_all(bucket, prefix).await
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

    pub(crate) async fn delete_all(&self, bucket: &str, prefix: &str) -> Result<()> {
        let disks = self.ctx.disks().read().await;

        let disks = disks.clone();

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

        Ok(())
    }
}
