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
use std::collections::HashSet;

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

impl ECStore {
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

        let _object_lock_guard = self.acquire_list_parts_read_lock(bucket, object, opts).await?;

        if self.single_pool() {
            return self.pools[0]
                .list_object_parts(bucket, object, upload_id, part_number_marker, max_parts, opts)
                .await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
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
        prefix: &str,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        delimiter: Option<String>,
        max_uploads: usize,
    ) -> Result<ListMultipartsInfo> {
        check_list_multipart_args(bucket, prefix, &key_marker, &upload_id_marker, &delimiter)?;

        if prefix.is_empty() {
            // TODO: return from cache
        }

        if self.single_pool() {
            return self.pools[0]
                .list_multipart_uploads(bucket, prefix, key_marker, upload_id_marker, delimiter, max_uploads)
                .await;
        }

        let mut uploads = Vec::new();
        let mut common_prefixes = HashSet::new();
        let mut source_truncated = false;

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
                continue;
            }
            let res = pool
                .list_multipart_uploads(
                    bucket,
                    prefix,
                    key_marker.clone(),
                    upload_id_marker.clone(),
                    delimiter.clone(),
                    max_uploads,
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
        let page = merge_multipart_upload_pages(uploads, common_prefixes.into_iter().collect(), max_uploads, source_truncated);

        Ok(ListMultipartsInfo {
            key_marker,
            upload_id_marker,
            next_key_marker: page.next_key_marker,
            next_upload_id_marker: page.next_upload_id_marker,
            max_uploads,
            is_truncated: page.is_truncated,
            uploads: page.uploads,
            common_prefixes: page.common_prefixes,
            prefix: prefix.to_owned(),
            delimiter: delimiter.to_owned(),
        })
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_new_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartUploadResult> {
        self.handle_new_multipart_upload_with_pool_idx(bucket, object, opts)
            .await
            .map(|(res, _)| res)
    }

    pub(crate) async fn handle_new_multipart_upload_with_pool_idx(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<(MultipartUploadResult, usize)> {
        check_new_multipart_args(bucket, object)?;

        if self.single_pool() {
            return self.pools[0]
                .new_multipart_upload(bucket, object, opts)
                .await
                .map(|res| (res, 0));
        }

        for (idx, pool) in self.pools.iter().enumerate() {
            if self.is_suspended(idx).await || self.is_pool_rebalancing(idx).await {
                continue;
            }
            let res = pool
                .list_multipart_uploads(bucket, object, None, None, None, MAX_UPLOADS_LIST)
                .await?;

            if !res.uploads.is_empty() {
                let res = self.pools[idx].new_multipart_upload(bucket, object, opts).await?;
                return Ok((res, idx));
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

        let res = self.pools[idx].new_multipart_upload(bucket, object, opts).await?;
        Ok((res, idx))
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
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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

        if self.single_pool() {
            return self.pools[0]
                .put_object_part(bucket, object, upload_id, part_id, data, opts)
                .await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
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

    #[instrument(skip(self))]
    pub(super) async fn handle_get_multipart_info(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartInfo> {
        check_list_parts_args(bucket, object, upload_id)?;
        if self.single_pool() {
            return self.pools[0].get_multipart_info(bucket, object, upload_id, opts).await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
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

        // TODO: defer DeleteUploadID

        if self.single_pool() {
            return self.pools[0].abort_multipart_upload(bucket, object, upload_id, opts).await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
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

        if self.single_pool() {
            return self.pools[0]
                .clone()
                .complete_multipart_upload(bucket, object, upload_id, uploaded_parts, opts)
                .await;
        }

        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
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
