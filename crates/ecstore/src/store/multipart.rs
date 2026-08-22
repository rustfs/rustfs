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

use super::*;
use crate::multipart_listing::paginate_multipart_listing;
use crate::set_disk::get_lock_acquire_timeout;
use crate::storage_api_contracts::multipart::MultipartOperations as _;
use futures::{StreamExt, stream};
use std::collections::HashSet;

const MULTIPART_LIST_SET_CONCURRENCY: usize = 4;

#[derive(Clone, Debug)]
pub(super) struct MultipartUploadListRequest {
    pub(super) prefix: String,
    pub(super) key_marker: Option<String>,
    pub(super) upload_id_marker: Option<String>,
    pub(super) delimiter: Option<String>,
    pub(super) max_uploads: usize,
    pub(super) expected_incarnation_id: Option<Uuid>,
}

fn map_multipart_namespace_lock_error(
    bucket: &str,
    object: &str,
    mode: &'static str,
    err: rustfs_lock::LockError,
) -> StorageError {
    match err {
        rustfs_lock::LockError::QuorumNotReached { required, achieved } => StorageError::NamespaceLockQuorumUnavailable {
            mode,
            bucket: bucket.to_string(),
            object: object.to_string(),
            required,
            achieved,
        },
        other => StorageError::Lock(other),
    }
}

fn ensure_multipart_bucket_lifecycle_guard_held(
    guard: Option<&rustfs_lock::NamespaceLockGuard>,
    bucket: &str,
    object: &str,
) -> Result<()> {
    if guard.is_some_and(rustfs_lock::NamespaceLockGuard::is_lock_lost) {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "multipart_bucket_generation",
            bucket: bucket.to_string(),
            object: object.to_string(),
            required: 1,
            achieved: 0,
        });
    }
    Ok(())
}

#[cfg(test)]
struct DataMovementMultipartCompletionBarrierState {
    bucket: String,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
pub(crate) struct DataMovementMultipartCompletionBarrier {
    state: Arc<DataMovementMultipartCompletionBarrierState>,
}

#[cfg(test)]
static DATA_MOVEMENT_MULTIPART_COMPLETION_BARRIER: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<DataMovementMultipartCompletionBarrierState>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
impl DataMovementMultipartCompletionBarrier {
    pub(crate) fn install(bucket: &str) -> Self {
        let state = Arc::new(DataMovementMultipartCompletionBarrierState {
            bucket: bucket.to_string(),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let mut slot = DATA_MOVEMENT_MULTIPART_COMPLETION_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("data movement multipart completion barrier mutex should not poison");
        assert!(slot.is_none(), "data movement multipart completion barrier must be unique");
        *slot = Some(Arc::clone(&state));
        Self { state }
    }

    pub(crate) async fn wait_until_paused(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("data movement multipart operation should reach selected completion");
    }
}

#[cfg(test)]
impl Drop for DataMovementMultipartCompletionBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = DATA_MOVEMENT_MULTIPART_COMPLETION_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("data movement multipart completion barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
async fn pause_data_movement_multipart_before_selected_completion(bucket: &str) {
    let barrier = DATA_MOVEMENT_MULTIPART_COMPLETION_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("data movement multipart completion barrier mutex should not poison")
        .as_ref()
        .filter(|barrier| barrier.bucket == bucket)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

async fn list_pool_multipart_uploads_for_incarnation(
    pool: &crate::core::sets::Sets,
    bucket: &str,
    request: &MultipartUploadListRequest,
) -> Result<ListMultipartsInfo> {
    let per_set_limit = request.max_uploads.saturating_add(1);
    let results = stream::iter(pool.disk_set.iter().cloned())
        .map(|set| {
            let request = request.clone();
            async move {
                set.list_multipart_uploads_for_incarnation(
                    bucket,
                    &request.prefix,
                    request.key_marker,
                    request.upload_id_marker,
                    request.delimiter,
                    per_set_limit,
                    request.expected_incarnation_id,
                )
                .await
            }
        })
        .buffer_unordered(MULTIPART_LIST_SET_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    let mut uploads = Vec::new();
    let mut common_prefixes = HashSet::new();
    let mut source_truncated = false;
    for result in results {
        let page = result?;
        uploads.extend(page.uploads);
        common_prefixes.extend(page.common_prefixes);
        source_truncated |= page.is_truncated;
    }

    let page = paginate_multipart_listing(
        uploads,
        common_prefixes.into_iter().collect(),
        request.key_marker.as_deref(),
        request.key_marker.as_ref().and(request.upload_id_marker.as_deref()),
        request.max_uploads,
        source_truncated,
    );

    Ok(ListMultipartsInfo {
        key_marker: request.key_marker.clone(),
        upload_id_marker: request.upload_id_marker.clone(),
        next_key_marker: page.next_key_marker,
        next_upload_id_marker: page.next_upload_id_marker,
        max_uploads: request.max_uploads,
        is_truncated: page.is_truncated,
        uploads: page.uploads,
        common_prefixes: page.common_prefixes,
        prefix: request.prefix.clone(),
        delimiter: request.delimiter.clone(),
    })
}

impl ECStore {
    #[allow(clippy::too_many_arguments)]
    pub async fn list_multipart_uploads_for_bucket_incarnation(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        delimiter: Option<String>,
        max_uploads: usize,
        expected_incarnation_id: Uuid,
    ) -> Result<ListMultipartsInfo> {
        self.handle_list_multipart_uploads(
            bucket,
            MultipartUploadListRequest {
                prefix: prefix.to_string(),
                key_marker,
                upload_id_marker,
                delimiter,
                max_uploads,
                expected_incarnation_id: Some(expected_incarnation_id),
            },
        )
        .await
    }

    /// Multipart lock order is bucket lifecycle, generation validation, then
    /// object/upload locks in the selected set.
    async fn guard_multipart_bucket_incarnation(
        &self,
        bucket: &str,
        opts: &ObjectOptions,
    ) -> Result<(ObjectOptions, Option<rustfs_lock::NamespaceLockGuard>)> {
        let mut opts = opts.clone();
        if is_meta_bucketname(bucket) {
            return Ok((opts, None));
        }
        if opts.expected_bucket_incarnation_id.is_none() {
            opts.expected_bucket_incarnation_id = Some(self.bucket_incarnation_id(bucket).await?);
        }
        let guard = if opts.bucket_lifecycle_lock_fence.is_some() {
            None
        } else {
            Some(self.acquire_bucket_lifecycle_read_lock(bucket).await?)
        };
        if let Some(guard) = guard.as_ref() {
            opts.add_bucket_lifecycle_lock_guard(guard);
        }
        let current = crate::bucket::metadata_sys::get_bucket_incarnation_id_in(&self.ctx, bucket).await?;
        if opts.expected_bucket_incarnation_id != Some(current) {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        Ok((opts, guard))
    }

    async fn acquire_list_parts_read_lock(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<Option<rustfs_lock::NamespaceLockGuard>> {
        if opts.no_lock {
            return Ok(None);
        }

        let ns_lock = self.handle_new_ns_lock(bucket, object).await?;
        ns_lock
            .get_read_lock(get_lock_acquire_timeout())
            .await
            .map(Some)
            .map_err(|err| map_multipart_namespace_lock_error(bucket, object, "read", err))
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_list_object_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: Option<usize>,
        max_parts: usize,
        opts: &ObjectOptions,
    ) -> Result<ListPartsInfo> {
        check_list_parts_args(bucket, object, upload_id)?;
        let (opts, _bucket_lifecycle_guard) = self.guard_multipart_bucket_incarnation(bucket, opts).await?;
        let opts = &opts;

        let _object_lock_guard = self.acquire_list_parts_read_lock(bucket, object, opts).await?;

        if self.single_pool() {
            return self.pools[0]
                .list_object_parts(bucket, object, upload_id, part_number_marker, max_parts, opts)
                .await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await || self.is_pool_rebalancing(pool.pool_idx).await {
                continue;
            }
            return match pool
                .list_object_parts(bucket, object, upload_id, part_number_marker, max_parts, opts)
                .await
            {
                Ok(res) => Ok(res),
                Err(err) => {
                    if is_err_invalid_upload_id(&err) {
                        continue;
                    }
                    Err(err)
                }
            };
        }

        Err(StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned()))
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_list_multipart_uploads(
        &self,
        bucket: &str,
        request: MultipartUploadListRequest,
    ) -> Result<ListMultipartsInfo> {
        check_list_multipart_args(
            bucket,
            &request.prefix,
            &request.key_marker,
            &request.upload_id_marker,
            &request.delimiter,
        )?;
        let guard_opts = ObjectOptions {
            expected_bucket_incarnation_id: request.expected_incarnation_id,
            ..Default::default()
        };
        let (opts, bucket_lifecycle_guard) = self.guard_multipart_bucket_incarnation(bucket, &guard_opts).await?;
        let expected_incarnation_id = opts.expected_bucket_incarnation_id;

        if request.prefix.is_empty() {
            // TODO(backlog): return cached multipart listing when prefix is empty
        }

        if self.single_pool() {
            let result = list_pool_multipart_uploads_for_incarnation(
                &self.pools[0],
                bucket,
                &MultipartUploadListRequest {
                    expected_incarnation_id,
                    ..request.clone()
                },
            )
            .await;
            ensure_multipart_bucket_lifecycle_guard_held(bucket_lifecycle_guard.as_ref(), bucket, &request.prefix)?;
            return result;
        }

        let mut uploads = Vec::new();
        let mut common_prefixes = HashSet::new();
        let mut source_truncated = false;

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await || self.is_pool_rebalancing(pool.pool_idx).await {
                continue;
            }
            let res = list_pool_multipart_uploads_for_incarnation(
                pool,
                bucket,
                &MultipartUploadListRequest {
                    expected_incarnation_id,
                    ..request.clone()
                },
            )
            .await?;
            uploads.extend(res.uploads);
            common_prefixes.extend(res.common_prefixes);
            source_truncated |= res.is_truncated;
        }

        // Each pool caps its own page at `max_uploads`, so the concatenation is
        // unordered across pools and may exceed the global cap. Re-sort, re-cap,
        // and derive the truncation markers so a bucket whose uploads span pools
        // pages correctly instead of being silently reported complete.
        let page =
            merge_multipart_upload_pages(uploads, common_prefixes.into_iter().collect(), request.max_uploads, source_truncated);
        ensure_multipart_bucket_lifecycle_guard_held(bucket_lifecycle_guard.as_ref(), bucket, &request.prefix)?;

        Ok(ListMultipartsInfo {
            key_marker: request.key_marker,
            upload_id_marker: request.upload_id_marker,
            next_key_marker: page.next_key_marker,
            next_upload_id_marker: page.next_upload_id_marker,
            max_uploads: request.max_uploads,
            is_truncated: page.is_truncated,
            uploads: page.uploads,
            common_prefixes: page.common_prefixes,
            prefix: request.prefix,
            delimiter: request.delimiter,
        })
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_new_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartUploadResult> {
        self.handle_new_multipart_upload_with_pool_idx(bucket, object, opts, None)
            .await
            .map(|(res, _, _)| res)
    }

    pub(crate) async fn handle_new_multipart_upload_with_pool_idx(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
        mutation_fence: Option<&ObjectLockDiagGuard>,
    ) -> Result<(MultipartUploadResult, usize, Option<Uuid>)> {
        check_new_multipart_args(bucket, object)?;
        let (mut opts, _bucket_lifecycle_guard) = self.guard_multipart_bucket_incarnation(bucket, opts).await?;

        if self.single_pool() {
            self.apply_decommission_target_mutation_fence(0, object, &mut opts, mutation_fence)
                .await;
            return self.pools[0]
                .new_multipart_upload(bucket, object, &opts)
                .await
                .map(|res| (res, 0, opts.expected_bucket_incarnation_id));
        }

        if opts.data_movement && opts.version_id.is_some() {
            let idx = self.select_data_movement_pool_idx(bucket, object, -1, &opts, false).await?;
            if idx == opts.src_pool_idx {
                return Err(StorageError::DataMovementOverwriteErr(
                    bucket.to_owned(),
                    object.to_owned(),
                    opts.version_id.clone().unwrap_or_default(),
                ));
            }
            self.apply_decommission_target_mutation_fence(idx, object, &mut opts, mutation_fence)
                .await;
            let res = self.pools[idx].new_multipart_upload(bucket, object, &opts).await?;
            return Ok((res, idx, opts.expected_bucket_incarnation_id));
        }

        for (idx, pool) in self.pools.iter().enumerate() {
            if self.is_suspended(idx).await || self.is_pool_rebalancing(idx).await {
                continue;
            }
            let res = list_pool_multipart_uploads_for_incarnation(
                pool,
                bucket,
                &MultipartUploadListRequest {
                    prefix: object.to_string(),
                    key_marker: None,
                    upload_id_marker: None,
                    delimiter: None,
                    max_uploads: MAX_UPLOADS_LIST,
                    expected_incarnation_id: opts.expected_bucket_incarnation_id,
                },
            )
            .await?;

            if !res.uploads.is_empty() {
                self.apply_decommission_target_mutation_fence(idx, object, &mut opts, mutation_fence)
                    .await;
                let res = self.pools[idx].new_multipart_upload(bucket, object, &opts).await?;
                return Ok((res, idx, opts.expected_bucket_incarnation_id));
            }
        }
        let idx = self.get_pool_idx(bucket, object, -1).await?;
        if opts.data_movement && idx == opts.src_pool_idx {
            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                "".to_owned(),
            ));
        }

        self.apply_decommission_target_mutation_fence(idx, object, &mut opts, mutation_fence)
            .await;
        let res = self.pools[idx].new_multipart_upload(bucket, object, &opts).await?;
        Ok((res, idx, opts.expected_bucket_incarnation_id))
    }

    #[instrument(skip(self))]
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_copy_object_part(
        &self,
        src_bucket: &str,
        src_object: &str,
        _dst_bucket: &str,
        _dst_object: &str,
        _upload_id: &str,
        _part_id: usize,
        _start_offset: i64,
        _length: i64,
        _src_info: &ObjectInfo,
        _src_opts: &ObjectOptions,
        _dst_opts: &ObjectOptions,
    ) -> Result<()> {
        check_new_multipart_args(src_bucket, src_object)?;

        // The full UploadPartCopy path still requires the higher S3/request layer to
        // derive encryption, compression, and multipart checksum write semantics.
        Err(StorageError::NotImplemented)
    }

    #[instrument(skip(self, data))]
    #[hotpath::measure(impl_type = "ECStore")]
    pub(super) async fn handle_put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo> {
        check_put_object_part_args(bucket, object, upload_id)?;
        let (opts, _bucket_lifecycle_guard) = self.guard_multipart_bucket_incarnation(bucket, opts).await?;
        let opts = &opts;

        if self.single_pool() {
            return self.pools[0]
                .put_object_part(bucket, object, upload_id, part_id, data, opts)
                .await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await || self.is_pool_rebalancing(pool.pool_idx).await {
                continue;
            }
            let err = match pool.put_object_part(bucket, object, upload_id, part_id, data, opts).await {
                Ok(res) => return Ok(res),
                Err(err) => {
                    if is_err_invalid_upload_id(&err) {
                        None
                    } else {
                        Some(err)
                    }
                }
            };

            if let Some(err) = err {
                error!("put_object_part err: {:?}", err);
                return Err(err);
            }
        }

        Err(StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned()))
    }

    pub(crate) async fn put_object_part_for_data_movement(
        &self,
        target_pool_idx: usize,
        bucket: &str,
        object: &str,
        upload_id: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo> {
        let part_id = opts
            .part_number
            .ok_or_else(|| Error::other("targeted multipart upload requires a part number"))?;
        check_put_object_part_args(bucket, object, upload_id)?;
        if !opts.data_movement {
            return Err(Error::other("targeted multipart upload requires data_movement options"));
        }
        let (opts, _bucket_lifecycle_guard) = self.guard_multipart_bucket_incarnation(bucket, opts).await?;
        let pool = self
            .pools
            .get(target_pool_idx)
            .ok_or_else(|| Error::other(format!("data movement target pool {target_pool_idx} is out of range")))?;
        pool.put_object_part(bucket, object, upload_id, part_id, data, &opts).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_get_multipart_info(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartInfo> {
        check_list_parts_args(bucket, object, upload_id)?;
        let (opts, _bucket_lifecycle_guard) = self.guard_multipart_bucket_incarnation(bucket, opts).await?;
        let opts = &opts;
        if self.single_pool() {
            return self.pools[0].get_multipart_info(bucket, object, upload_id, opts).await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await || self.is_pool_rebalancing(pool.pool_idx).await {
                continue;
            }

            return match pool.get_multipart_info(bucket, object, upload_id, opts).await {
                Ok(res) => Ok(res),
                Err(err) => {
                    if is_err_invalid_upload_id(&err) {
                        continue;
                    }

                    Err(err)
                }
            };
        }

        Err(StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned()))
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_abort_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<()> {
        check_abort_multipart_args(bucket, object, upload_id)?;
        let (opts, _bucket_lifecycle_guard) = self.guard_multipart_bucket_incarnation(bucket, opts).await?;
        let opts = &opts;

        // TODO(backlog): defer DeleteUploadID to background for faster abort response

        if self.single_pool() {
            return self.pools[0].abort_multipart_upload(bucket, object, upload_id, opts).await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await || self.is_pool_rebalancing(pool.pool_idx).await {
                continue;
            }

            let err = match pool.abort_multipart_upload(bucket, object, upload_id, opts).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    //
                    if is_err_invalid_upload_id(&err) { None } else { Some(err) }
                }
            };

            if let Some(er) = err {
                return Err(er);
            }
        }

        Err(StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned()))
    }

    pub(crate) async fn abort_multipart_upload_for_data_movement(
        &self,
        target_pool_idx: usize,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<()> {
        check_abort_multipart_args(bucket, object, upload_id)?;
        if !opts.data_movement {
            return Err(Error::other("targeted multipart abort requires data_movement options"));
        }
        let (opts, _bucket_lifecycle_guard) = self.guard_multipart_bucket_incarnation(bucket, opts).await?;
        let pool = self
            .pools
            .get(target_pool_idx)
            .ok_or_else(|| Error::other(format!("data movement target pool {target_pool_idx} is out of range")))?;
        pool.abort_multipart_upload(bucket, object, upload_id, &opts).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_complete_multipart_upload(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        check_complete_multipart_args(bucket, object, upload_id)?;
        let (opts, _bucket_lifecycle_guard) = self.guard_multipart_bucket_incarnation(bucket, opts).await?;
        let opts = &opts;

        if self.single_pool() {
            return self.pools[0]
                .clone()
                .complete_multipart_upload(bucket, object, upload_id, uploaded_parts, opts)
                .await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await || self.is_pool_rebalancing(pool.pool_idx).await {
                continue;
            }

            let pool = pool.clone();
            let err = match pool
                .complete_multipart_upload(bucket, object, upload_id, uploaded_parts.clone(), opts)
                .await
            {
                Ok(res) => return Ok(res),
                Err(err) => {
                    //
                    if is_err_invalid_upload_id(&err) { None } else { Some(err) }
                }
            };

            if let Some(er) = err {
                return Err(er);
            }
        }

        Err(StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned()))
    }

    pub(crate) async fn complete_multipart_upload_for_data_movement(
        self: Arc<Self>,
        target: (usize, Option<&ObjectLockDiagGuard>),
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let (target_pool_idx, mutation_fence) = target;
        check_complete_multipart_args(bucket, object, upload_id)?;
        if !opts.data_movement {
            return Err(Error::other("targeted multipart completion requires data_movement options"));
        }
        let (mut opts, _bucket_lifecycle_guard) = self.guard_multipart_bucket_incarnation(bucket, opts).await?;
        if opts.overwrites_existing_version() && !is_meta_bucketname(bucket) {
            let expected_incarnation_id = opts
                .expected_bucket_incarnation_id
                .ok_or_else(|| Error::other("data movement completion is missing its bucket incarnation"))?;
            let lifecycle_fence = opts
                .bucket_lifecycle_lock_fence
                .as_ref()
                .ok_or_else(|| Error::other("data movement completion is missing its bucket lifecycle fence"))?;
            let snapshot = match opts.object_lock_config_snapshot.as_ref() {
                Some(snapshot) => Arc::clone(snapshot),
                None => {
                    self.object_lock_config_snapshot_under_lifecycle_fence(bucket, lifecycle_fence)
                        .await?
                }
            };
            if !snapshot.is_valid_for_destructive_put(self.id, bucket, expected_incarnation_id) {
                return Err(Error::other(
                    "data movement Object Lock snapshot does not match the target bucket generation",
                ));
            }
            snapshot.add_lock_fences(&mut opts);
            opts.object_lock_config_snapshot = Some(snapshot);
        }
        self.apply_decommission_target_mutation_fence(target_pool_idx, object, &mut opts, mutation_fence)
            .await;
        #[cfg(test)]
        pause_data_movement_multipart_before_selected_completion(bucket).await;
        let pool = self
            .pools
            .get(target_pool_idx)
            .ok_or_else(|| Error::other(format!("data movement target pool {target_pool_idx} is out of range")))?
            .clone();
        let result = enqueue_transition_after_write(
            pool.complete_multipart_upload(bucket, object, upload_id, uploaded_parts, &opts)
                .await,
            LcEventSrc::S3CompleteMultipartUpload,
        )
        .await;
        if result.is_ok() {
            list_objects::observe_list_objects_mutation(self.as_ref(), bucket).await;
        }
        result
    }
}

/// Merges per-pool `ListMultipartUploads` pages into a single globally paginated
/// page.
///
/// Each pool independently applies the `max_uploads` cap, so the concatenated
/// input can hold up to `pools * max_uploads` entries and is unordered across
/// pools. This re-sorts the union by `(key, upload_id)` — the order S3 clients
/// page through — caps it to `max_uploads`, and derives the truncation markers
/// from the first overflow element (used only as a probe, never returned) so a
/// bucket whose uploads span pools can be paged without loss or duplication.
fn merge_multipart_upload_pages(
    uploads: Vec<MultipartInfo>,
    common_prefixes: Vec<String>,
    max_uploads: usize,
    source_truncated: bool,
) -> crate::multipart_listing::MultipartListingPage {
    paginate_multipart_listing(uploads, common_prefixes, None, None, max_uploads, source_truncated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{
        endpoints::{Endpoints, PoolEndpoints},
        format::FormatV3,
    };
    use std::time::Duration;

    fn mp(object: &str, upload_id: &str) -> MultipartInfo {
        MultipartInfo {
            bucket: "bucket".to_string(),
            object: object.to_string(),
            upload_id: upload_id.to_string(),
            initiated: None,
            ..Default::default()
        }
    }

    /// Models a single pool's `list_multipart_uploads`: returns uploads strictly
    /// after the `(key, upload_id)` marker in `(key, upload_id)` order, capped at
    /// `max_uploads` (mirroring the per-pool page cap).
    fn pool_query(
        pool: &[MultipartInfo],
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        max_uploads: usize,
    ) -> Vec<MultipartInfo> {
        pool.iter()
            .filter(|u| match (key_marker, upload_id_marker) {
                (Some(k), Some(uid)) => (u.object.as_str(), u.upload_id.as_str()) > (k, uid),
                (Some(k), None) => u.object.as_str() > k,
                _ => true,
            })
            .take(max_uploads)
            .cloned()
            .collect()
    }

    #[test]
    fn merge_multipart_upload_pages_sorts_and_caps_across_pools() {
        // Union of two pools, unordered and exceeding the global cap.
        let uploads = vec![mp("b", "u1"), mp("a", "u2"), mp("a", "u1"), mp("c", "u1"), mp("b", "u2")];

        let page = merge_multipart_upload_pages(uploads, Vec::new(), 3, false);

        assert!(page.is_truncated);
        assert_eq!(page.uploads.len(), 3);
        let ordered: Vec<(&str, &str)> = page
            .uploads
            .iter()
            .map(|u| (u.object.as_str(), u.upload_id.as_str()))
            .collect();
        assert_eq!(ordered, vec![("a", "u1"), ("a", "u2"), ("b", "u1")]);
        assert_eq!(page.next_key_marker.as_deref(), Some("b"));
        assert_eq!(page.next_upload_id_marker.as_deref(), Some("u1"));
    }

    #[test]
    fn merge_multipart_upload_pages_reports_complete_within_cap() {
        let uploads = vec![mp("b", "u1"), mp("a", "u1")];

        let page = merge_multipart_upload_pages(uploads, Vec::new(), 3, false);

        assert_eq!(page.uploads.len(), 2);
        assert!(!page.is_truncated);
        assert!(page.next_key_marker.is_none());
        assert!(page.next_upload_id_marker.is_none());
    }

    #[test]
    fn merge_multipart_upload_pages_paginates_across_pools_without_loss() {
        // Uploads for the same bucket spread across two pools, together exceeding
        // the cap, so pagination must span multiple pages.
        let pool0 = vec![mp("a", "u1"), mp("a", "u3"), mp("c", "u1"), mp("e", "u1")];
        let pool1 = vec![mp("a", "u2"), mp("b", "u1"), mp("d", "u1"), mp("f", "u1")];

        let mut expected: Vec<(String, String)> = pool0
            .iter()
            .chain(pool1.iter())
            .map(|u| (u.object.clone(), u.upload_id.clone()))
            .collect();
        expected.sort();

        let max_uploads = 3;
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;
        let mut collected: Vec<(String, String)> = Vec::new();

        for _ in 0..16 {
            let mut merged = Vec::new();
            for pool in [&pool0, &pool1] {
                merged.extend(pool_query(pool, key_marker.as_deref(), upload_id_marker.as_deref(), max_uploads));
            }

            let page = merge_multipart_upload_pages(merged, Vec::new(), max_uploads, false);
            assert!(page.uploads.len() <= max_uploads);
            collected.extend(page.uploads.iter().map(|u| (u.object.clone(), u.upload_id.clone())));

            if !page.is_truncated {
                break;
            }
            key_marker = page.next_key_marker;
            upload_id_marker = page.next_upload_id_marker;
        }

        assert_eq!(collected, expected, "pagination must return every upload exactly once, in sorted order");
        let mut deduped = collected.clone();
        deduped.dedup();
        assert_eq!(deduped.len(), collected.len(), "pagination must not duplicate uploads");
    }

    #[test]
    fn merge_multipart_upload_pages_includes_common_prefixes() {
        let page = merge_multipart_upload_pages(
            vec![mp("logs/root.bin", "u1")],
            vec!["logs/2026/".to_string(), "logs/2025/".to_string()],
            2,
            false,
        );

        assert!(page.is_truncated);
        assert!(page.uploads.is_empty());
        assert_eq!(page.common_prefixes, vec!["logs/2025/", "logs/2026/"]);
        assert_eq!(page.next_key_marker.as_deref(), Some("logs/2026/"));
        assert!(page.next_upload_id_marker.is_none());
    }

    #[test]
    fn merge_multipart_upload_pages_preserves_pool_truncation() {
        let page = merge_multipart_upload_pages(vec![mp("a", "u1")], Vec::new(), 1, true);

        assert!(page.is_truncated);
        assert_eq!(page.next_key_marker.as_deref(), Some("a"));
        assert_eq!(page.next_upload_id_marker.as_deref(), Some("u1"));
    }

    async fn new_multipart_lock_test_store() -> ECStore {
        let format = FormatV3::new(1, 2);
        let endpoints = vec![
            Endpoint::try_from("http://127.0.0.1:9000/data0").expect("first endpoint should parse"),
            Endpoint::try_from("http://127.0.0.1:9001/data1").expect("second endpoint should parse"),
        ];
        let pool_endpoints = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 2,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "multipart-list-parts-lock-test".to_string(),
            platform: "test".to_string(),
        };
        let endpoint_pools = EndpointServerPools::from(vec![pool_endpoints.clone()]);
        let sets = Sets::new(vec![None, None], &pool_endpoints, &format, 0, 1)
            .await
            .expect("test sets should be created with empty disks");

        ECStore {
            id: Uuid::new_v4(),
            disk_map: HashMap::new(),
            pools: vec![sets],
            peer_sys: S3PeerSys::new(&endpoint_pools),
            pool_meta: RwLock::new(PoolMeta::default()),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers: RwLock::new(Vec::new()),
            start_gate: Mutex::new(()),
            pool_meta_save_gate: Mutex::new(()),
            ctx: crate::runtime::instance::bootstrap_ctx(),
            bucket_fence_registry: std::sync::Arc::default(),
        }
    }

    #[tokio::test]
    async fn list_parts_read_lock_blocks_object_writer_until_released() {
        let store = new_multipart_lock_test_store().await;
        let read_guard = store
            .acquire_list_parts_read_lock("bucket", "object", &ObjectOptions::default())
            .await
            .expect("list parts read lock should be acquired")
            .expect("default options should acquire a read lock");

        let object_lock = store
            .handle_new_ns_lock("bucket", "object")
            .await
            .expect("object namespace lock should be created");
        let err = object_lock
            .get_write_lock(Duration::from_millis(20))
            .await
            .expect_err("list parts read lock should block object writers");
        assert!(matches!(err, rustfs_lock::LockError::Timeout { .. }));

        drop(read_guard);
        object_lock
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("object writer should proceed after list parts releases the read lock");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn list_parts_read_lock_respects_no_lock() {
        let store = new_multipart_lock_test_store().await;
        let object_lock = store
            .handle_new_ns_lock("bucket", "object")
            .await
            .expect("object namespace lock should be created");
        let _writer = object_lock
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("outer write lock should be acquired");

        let result = temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1"))], async {
            store
                .acquire_list_parts_read_lock("bucket", "object", &ObjectOptions::default())
                .await
        })
        .await;
        let err = match result {
            Ok(_) => panic!("list parts read lock must wait behind an object writer"),
            Err(err) => err,
        };
        assert!(matches!(err, StorageError::Lock(rustfs_lock::LockError::Timeout { .. })));

        let no_lock_guard = store
            .acquire_list_parts_read_lock(
                "bucket",
                "object",
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("no_lock list parts path should not acquire an object lock");
        assert!(no_lock_guard.is_none());
    }
}
