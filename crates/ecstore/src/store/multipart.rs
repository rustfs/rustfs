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

fn should_reuse_existing_multipart_pool(pool_idx: usize, opts: &ObjectOptions) -> bool {
    !(opts.data_movement && pool_idx == opts.src_pool_idx)
}

fn multipart_data_movement_overwrite_err(bucket: &str, object: &str, opts: &ObjectOptions) -> Error {
    StorageError::DataMovementOverwriteErr(bucket.to_owned(), object.to_owned(), opts.version_id.clone().unwrap_or_default())
}

impl ECStore {
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

        // TODO: nslock

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
        }

        Ok(ListMultipartsInfo {
            key_marker,
            upload_id_marker,
            max_uploads,
            uploads,
            prefix: prefix.to_owned(),
            delimiter: delimiter.to_owned(),
            ..Default::default()
        })
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_new_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartUploadResult> {
        check_new_multipart_args(bucket, object)?;

        if self.single_pool() {
            return self.pools[0].new_multipart_upload(bucket, object, opts).await;
        }

        for (idx, pool) in self.pools.iter().enumerate() {
            if self.is_suspended(idx).await || self.is_pool_rebalancing(idx).await {
                continue;
            }
            let res = pool
                .list_multipart_uploads(bucket, object, None, None, None, MAX_UPLOADS_LIST)
                .await?;

            if !res.uploads.is_empty() {
                if !should_reuse_existing_multipart_pool(idx, opts) {
                    continue;
                }
                return self.pools[idx].new_multipart_upload(bucket, object, opts).await;
            }
        }
        let idx = if opts.data_movement && opts.version_id.is_some() {
            self.select_data_movement_pool_idx(bucket, object, -1, opts, false).await?
        } else {
            self.get_pool_idx(bucket, object, -1).await?
        };
        if opts.data_movement && idx == opts.src_pool_idx {
            return Err(multipart_data_movement_overwrite_err(bucket, object, opts));
        }

        self.pools[idx].new_multipart_upload(bucket, object, opts).await
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

        // TODO: PutObjectReader
        // self.put_object_part(dst_bucket, dst_object, upload_id, part_id, data, opts)

        Err(StorageError::NotImplemented)
    }

    #[instrument(skip(self, data))]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reuse_existing_multipart_pool_allows_regular_uploads_on_source_pool() {
        let opts = ObjectOptions {
            src_pool_idx: 1,
            ..Default::default()
        };

        assert!(should_reuse_existing_multipart_pool(1, &opts));
    }

    #[test]
    fn should_reuse_existing_multipart_pool_rejects_data_movement_source_pool() {
        let opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 1,
            ..Default::default()
        };

        assert!(!should_reuse_existing_multipart_pool(1, &opts));
    }

    #[test]
    fn should_reuse_existing_multipart_pool_allows_data_movement_target_pool() {
        let opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 1,
            ..Default::default()
        };

        assert!(should_reuse_existing_multipart_pool(2, &opts));
    }

    #[test]
    fn multipart_data_movement_overwrite_err_preserves_version_id() {
        let err = multipart_data_movement_overwrite_err(
            "bucket",
            "object",
            &ObjectOptions {
                version_id: Some("vid-1".to_string()),
                ..Default::default()
            },
        );

        assert_eq!(
            err,
            Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), "vid-1".to_string())
        );
    }

    #[test]
    fn multipart_data_movement_overwrite_err_defaults_empty_version_id() {
        let err = multipart_data_movement_overwrite_err("bucket", "object", &ObjectOptions::default());

        assert_eq!(
            err,
            Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), String::new())
        );
    }
}
