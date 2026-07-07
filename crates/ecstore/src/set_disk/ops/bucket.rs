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

//! `BucketOperations` for `SetDisks`.
//!
//! P4 of the SetDisks God-Object split (tracking backlog#815, issue #819).
//! Relocated verbatim from `set_disk/mod.rs`; the contract stays implemented
//! `for SetDisks`, so its associated-type bounds are unchanged and runtime
//! behavior is the same.

use super::super::*;

#[async_trait::async_trait]
impl BucketOperations for SetDisks {
    type Error = Error;

    #[tracing::instrument(skip(self))]
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()> {
        let disks = self.disk_inventory().await;
        let write_quorum = (disks.len() / 2) + 1;
        let force_create = opts.force_create;

        let mut futures = Vec::with_capacity(disks.len());
        for disk in disks {
            let bucket = bucket.to_string();
            futures.push(async move {
                match disk {
                    Some(disk) => match disk.make_volume(&bucket).await {
                        Ok(()) => Ok(()),
                        Err(err) if force_create && matches!(err, DiskError::VolumeExists) => Ok(()),
                        Err(err) => Err(err),
                    },
                    None => Err(DiskError::DiskNotFound),
                }
            });
        }

        let results = join_all(futures).await;
        let errs = results
            .into_iter()
            .map(|result| result.err())
            .collect::<Vec<Option<DiskError>>>();

        if let Some(err) = reduce_write_quorum_errs(&errs, BUCKET_OP_IGNORED_ERRS, write_quorum) {
            return Err(err.into());
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get_bucket_info(&self, bucket: &str, _opts: &BucketOptions) -> Result<BucketInfo> {
        let disks = self.disk_inventory().await;
        let write_quorum = (disks.len() / 2) + 1;

        let mut futures = Vec::with_capacity(disks.len());
        for disk in disks {
            let bucket = bucket.to_string();
            futures.push(async move {
                match disk {
                    Some(disk) => disk.stat_volume(&bucket).await,
                    None => Err(DiskError::DiskNotFound),
                }
            });
        }

        let results = join_all(futures).await;
        let mut infos = Vec::with_capacity(results.len());
        let mut errs = Vec::with_capacity(results.len());
        for result in results {
            match result {
                Ok(info) => {
                    infos.push(Some(info));
                    errs.push(None);
                }
                Err(err) => {
                    infos.push(None);
                    errs.push(Some(err));
                }
            }
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, BUCKET_OP_IGNORED_ERRS, write_quorum) {
            return Err(err.into());
        }

        let mut versioning = false;
        let mut object_locking = false;
        if let Ok(sys) = metadata_sys::get(bucket).await {
            versioning = sys.versioning();
            object_locking = sys.object_locking();
        }

        infos
            .into_iter()
            .flatten()
            .next()
            .map(|info| BucketInfo {
                name: info.name,
                created: info.created,
                versioning,
                object_locking,
                ..Default::default()
            })
            .ok_or(Error::VolumeNotFound)
    }

    #[tracing::instrument(skip(self))]
    async fn list_bucket(&self, _opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
        let disks = self.disk_inventory().await;
        let write_quorum = (disks.len() / 2) + 1;

        let mut futures = Vec::with_capacity(disks.len());
        for disk in disks {
            futures.push(async move {
                match disk {
                    Some(disk) => disk.list_volumes().await,
                    None => Err(DiskError::DiskNotFound),
                }
            });
        }

        let results = join_all(futures).await;
        let mut infos = Vec::with_capacity(results.len());
        let mut errs = Vec::with_capacity(results.len());
        for result in results {
            match result {
                Ok(volumes) => {
                    infos.push(Some(volumes));
                    errs.push(None);
                }
                Err(err) => {
                    infos.push(None);
                    errs.push(Some(err));
                }
            }
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, BUCKET_OP_IGNORED_ERRS, write_quorum) {
            return Err(err.into());
        }

        let mut counts: HashMap<String, (usize, BucketInfo)> = HashMap::new();
        for volumes in infos.into_iter().flatten() {
            for volume in volumes {
                if is_reserved_or_invalid_bucket(&volume.name, false) {
                    continue;
                }

                let entry = counts.entry(volume.name.clone()).or_insert((
                    0,
                    BucketInfo {
                        name: volume.name.clone(),
                        created: volume.created,
                        ..Default::default()
                    },
                ));
                entry.0 += 1;
            }
        }

        let mut buckets = counts
            .into_values()
            .filter_map(|(count, bucket)| (count >= write_quorum).then_some(bucket))
            .collect::<Vec<_>>();
        buckets.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(buckets)
    }

    #[tracing::instrument(skip(self))]
    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()> {
        let disks = self.disk_inventory().await;
        let write_quorum = (disks.len() / 2) + 1;

        let mut futures = Vec::with_capacity(disks.len());
        for disk in disks.iter().cloned() {
            let bucket = bucket.to_string();
            let force = opts.force;
            futures.push(async move {
                match disk {
                    // Non-force refuses a non-empty bucket (VolumeNotEmpty); only
                    // an explicit force delete removes recursively (backlog#799 B1).
                    Some(disk) => disk.delete_volume(&bucket, force).await,
                    None => Err(DiskError::DiskNotFound),
                }
            });
        }

        let results = join_all(futures).await;
        let mut errs = Vec::with_capacity(results.len());
        let mut recreate = false;
        for result in results {
            match result {
                Ok(()) => errs.push(None),
                Err(err) => {
                    if matches!(err, DiskError::VolumeNotEmpty) {
                        recreate = true;
                    }
                    errs.push(Some(err));
                }
            }
        }

        if recreate {
            for (index, err) in errs.iter().enumerate() {
                if err.is_none()
                    && let Some(Some(disk)) = disks.get(index)
                {
                    let _ = disk.make_volume(bucket).await;
                }
            }
            return Err(Error::VolumeNotEmpty);
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, BUCKET_OP_IGNORED_ERRS, write_quorum) {
            return Err(err.into());
        }

        Ok(())
    }
}
