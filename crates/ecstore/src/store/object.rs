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

fn select_data_movement_target_pool(
    existing_pool_idx: Result<usize>,
    src_pool_idx: usize,
    delete_marker: bool,
) -> Result<Option<usize>> {
    match existing_pool_idx {
        Ok(pool_idx) => {
            if delete_marker && pool_idx == src_pool_idx {
                Ok(None)
            } else {
                Ok(Some(pool_idx))
            }
        }
        Err(err) => {
            if is_err_read_quorum(&err) {
                return Err(StorageError::ErasureWriteQuorum);
            }
            if delete_marker && (is_err_object_not_found(&err) || is_err_version_not_found(&err)) {
                Ok(None)
            } else {
                Err(err)
            }
        }
    }
}

fn latest_object_access_delete_marker_error(
    bucket: &str,
    object: &str,
    info: &ObjectInfo,
    opts: &ObjectOptions,
) -> Option<Error> {
    if !info.delete_marker {
        return None;
    }

    Some(if opts.version_id.is_none() || opts.delete_marker {
        to_object_err(StorageError::FileNotFound, vec![bucket, object])
    } else {
        to_object_err(StorageError::MethodNotAllowed, vec![bucket, object])
    })
}

fn resolve_latest_object_access(
    bucket: &str,
    object: &str,
    info: ObjectInfo,
    idx: usize,
    opts: &ObjectOptions,
) -> Result<(ObjectInfo, usize)> {
    if let Some(err) = latest_object_access_delete_marker_error(bucket, object, &info, opts) {
        return Err(err);
    }

    Ok((info, idx))
}

fn version_aware_lookup_opts(opts: &ObjectOptions, no_lock: bool) -> ObjectOptions {
    let mut lookup_opts = opts.clone();
    lookup_opts.no_lock = no_lock;
    if lookup_opts.version_id.is_some() {
        lookup_opts.metadata_chg = true;
    }

    lookup_opts
}

fn data_movement_pool_lookup_opts(opts: &ObjectOptions, no_lock: bool) -> ObjectOptions {
    let mut lookup_opts = version_aware_lookup_opts(opts, no_lock);
    lookup_opts.skip_decommissioned = true;
    lookup_opts.skip_rebalancing = true;

    lookup_opts
}

impl ECStore {
    async fn get_latest_accessible_object_info_with_idx(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<(ObjectInfo, usize)> {
        let (info, idx) = self.get_latest_object_info_with_idx(bucket, object, opts).await?;
        resolve_latest_object_access(bucket, object, info, idx, opts)
    }

    pub(super) async fn select_data_movement_pool_idx(
        &self,
        bucket: &str,
        object: &str,
        size: i64,
        opts: &ObjectOptions,
        no_lock: bool,
    ) -> Result<usize> {
        match self
            .get_pool_info_existing_with_opts(bucket, object, &data_movement_pool_lookup_opts(opts, no_lock))
            .await
        {
            Ok((pinfo, _)) => Ok(pinfo.index),
            Err(err) => {
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(err);
                }

                self.get_available_pool_idx(bucket, object, size).await.ok_or(Error::DiskFull)
            }
        }
    }

    fn resolve_decommission_target_pool_idx_result(result: Result<usize>, bucket: &str, object: &str) -> Result<usize> {
        result.map_err(|err| Error::other(format!("failed to select decommission target pool for {bucket}/{object}: {err}")))
    }

    fn resolve_decommission_tiered_object_result(result: Result<()>, bucket: &str, object: &str) -> Result<()> {
        result.map_err(|err| Error::other(format!("failed to decommission tiered object for {bucket}/{object}: {err}")))
    }

    #[instrument(skip(self, fi, opts))]
    pub(crate) async fn decommission_tiered_object(
        &self,
        bucket: &str,
        object: &str,
        fi: &rustfs_filemeta::FileInfo,
        opts: &ObjectOptions,
    ) -> Result<()> {
        check_put_object_args(bucket, object)?;

        let object = encode_dir_object(object);

        if self.single_pool() {
            return Self::resolve_decommission_tiered_object_result(
                Err(Error::other("single pool deployments cannot decommission tiered objects")),
                bucket,
                &object,
            );
        }

        let idx = if opts.data_movement && opts.version_id.is_some() {
            Self::resolve_decommission_target_pool_idx_result(
                self.select_data_movement_pool_idx(bucket, &object, fi.size, opts, true).await,
                bucket,
                &object,
            )?
        } else {
            Self::resolve_decommission_target_pool_idx_result(
                self.get_pool_idx_no_lock(bucket, &object, fi.size).await,
                bucket,
                &object,
            )?
        };
        if opts.data_movement && idx == opts.src_pool_idx {
            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.clone().unwrap_or_default(),
            ));
        }

        Self::resolve_decommission_tiered_object_result(
            self.pools[idx]
                .get_disks_by_key(&object)
                .decommission_tiered_object(bucket, &object, fi, opts)
                .await,
            bucket,
            &object,
        )
    }

    #[instrument(level = "debug", skip(self))]
    pub(super) async fn handle_get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        check_get_obj_args(bucket, object)?;

        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].get_object_reader(bucket, object.as_str(), range, h, opts).await;
        }

        // TODO: nslock

        let mut opts = opts.clone();

        opts.no_lock = true;

        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, &object, &opts)
            .await?;
        self.pools[idx]
            .get_object_reader(bucket, object.as_str(), range, h, &opts)
            .await
    }

    #[instrument(level = "debug", skip(self, data))]
    pub(super) async fn handle_put_object(
        &self,
        bucket: &str,
        object: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        check_put_object_args(bucket, object)?;

        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].put_object(bucket, object.as_str(), data, opts).await;
        }

        let idx = if opts.data_movement && opts.version_id.is_some() {
            self.select_data_movement_pool_idx(bucket, &object, data.size(), opts, false)
                .await?
        } else {
            self.get_pool_idx(bucket, &object, data.size()).await?
        };

        if opts.data_movement && idx == opts.src_pool_idx {
            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.clone().unwrap_or_default(),
            ));
        }

        self.pools[idx].put_object(bucket, &object, data, opts).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        check_object_args(bucket, object)?;

        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].get_object_info(bucket, object.as_str(), opts).await;
        }

        // TODO: nslock

        let (info, _) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), opts)
            .await?;
        opts.precondition_check(&info)?;
        Ok(info)
    }

    #[instrument(skip(self))]
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_copy_object(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        src_info: &mut ObjectInfo,
        src_opts: &ObjectOptions,
        dst_opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        check_copy_obj_args(src_bucket, src_object)?;
        check_copy_obj_args(dst_bucket, dst_object)?;

        let src_object = encode_dir_object(src_object);
        let dst_object = encode_dir_object(dst_object);

        let cp_src_dst_same = path_join_buf(&[src_bucket, &src_object]) == path_join_buf(&[dst_bucket, &dst_object]);

        // TODO: nslock

        let pool_idx = self
            .get_pool_info_existing_with_opts(src_bucket, &src_object, &version_aware_lookup_opts(src_opts, true))
            .await?
            .0
            .index;

        if cp_src_dst_same {
            if let (Some(src_vid), Some(dst_vid)) = (&src_opts.version_id, &dst_opts.version_id)
                && src_vid == dst_vid
            {
                return self.pools[pool_idx]
                    .copy_object(src_bucket, &src_object, dst_bucket, &dst_object, src_info, src_opts, dst_opts)
                    .await;
            }

            if !dst_opts.versioned && src_opts.version_id.is_none() {
                return self.pools[pool_idx]
                    .copy_object(src_bucket, &src_object, dst_bucket, &dst_object, src_info, src_opts, dst_opts)
                    .await;
            }

            if dst_opts.versioned && src_opts.version_id != dst_opts.version_id {
                src_info.version_only = true;
                return self.pools[pool_idx]
                    .copy_object(src_bucket, &src_object, dst_bucket, &dst_object, src_info, src_opts, dst_opts)
                    .await;
            }
        }

        let put_opts = ObjectOptions {
            user_defined: src_info.user_defined.clone(),
            versioned: dst_opts.versioned,
            version_id: dst_opts.version_id.clone(),
            no_lock: true,
            mod_time: dst_opts.mod_time,
            ..Default::default()
        };

        if let Some(put_object_reader) = src_info.put_object_reader.as_mut() {
            return self.pools[pool_idx]
                .put_object(dst_bucket, &dst_object, put_object_reader, &put_opts)
                .await;
        }

        Err(StorageError::InvalidArgument(
            src_bucket.to_owned(),
            src_object.to_owned(),
            "put_object_reader is none".to_owned(),
        ))
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo> {
        check_del_obj_args(bucket, object)?;

        if opts.delete_prefix {
            self.delete_prefix(bucket, object).await?;
            return Ok(ObjectInfo::default());
        }

        // TODO: nslock

        let object = encode_dir_object(object);
        let object = object.as_str();

        let gopts = version_aware_lookup_opts(&opts, true);

        if opts.data_movement {
            let existing_pool_idx = self
                .get_pool_info_existing_with_opts(bucket, object, &gopts)
                .await
                .map(|(pinfo, _)| pinfo.index);
            let target_pool_idx =
                match select_data_movement_target_pool(existing_pool_idx, opts.src_pool_idx, opts.delete_marker)? {
                    Some(pool_idx) => pool_idx,
                    None => self.get_pool_idx_no_lock(bucket, object, 0).await?,
                };

            if opts.src_pool_idx == target_pool_idx {
                return Err(StorageError::DataMovementOverwriteErr(
                    bucket.to_owned(),
                    object.to_owned(),
                    opts.version_id.unwrap_or_default(),
                ));
            }

            let mut obj = self.pools[target_pool_idx].delete_object(bucket, object, opts).await?;
            obj.name = decode_dir_object(obj.name.as_str());
            return Ok(obj);
        }

        // Determine which pool contains it
        let (mut pinfo, errs) = self
            .get_pool_info_existing_with_opts(bucket, object, &gopts)
            .await
            .map_err(|e| {
                if is_err_read_quorum(&e) {
                    StorageError::ErasureWriteQuorum
                } else {
                    e
                }
            })?;

        if pinfo.object_info.delete_marker && opts.version_id.is_none() {
            pinfo.object_info.name = decode_dir_object(object);
            return Ok(pinfo.object_info);
        }

        if opts.data_movement && opts.src_pool_idx == pinfo.index {
            return Err(StorageError::DataMovementOverwriteErr(
                bucket.to_owned(),
                object.to_owned(),
                opts.version_id.unwrap_or_default(),
            ));
        }

        if !errs.is_empty() && !opts.versioned && !opts.version_suspended {
            let mut obj = self.delete_object_from_all_pools(bucket, object, &opts, errs).await?;
            obj.name = decode_dir_object(object);
            return Ok(obj);
        }

        for pool in self.pools.iter() {
            match pool.delete_object(bucket, object, opts.clone()).await {
                Ok(res) => {
                    let mut obj = res;
                    obj.name = decode_dir_object(object);
                    return Ok(obj);
                }
                Err(err) => {
                    if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                        return Err(err);
                    }
                }
            }
        }

        if let Some(ver) = opts.version_id {
            return Err(StorageError::VersionNotFound(bucket.to_owned(), object.to_owned(), ver));
        }

        Err(StorageError::ObjectNotFound(bucket.to_owned(), object.to_owned()))
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
        // encode object name
        let objects: Vec<ObjectToDelete> = objects
            .iter()
            .map(|v| {
                let mut v = v.clone();
                v.object_name = encode_dir_object(v.object_name.as_str());
                v
            })
            .collect();

        // Default return value
        let mut del_objects = vec![DeletedObject::default(); objects.len()];

        let mut del_errs = Vec::with_capacity(objects.len());
        for _ in 0..objects.len() {
            del_errs.push(None)
        }

        // TODO: nslock

        let mut futures = Vec::with_capacity(self.pools.len());

        for pool in self.pools.iter() {
            futures.push(pool.delete_objects(bucket, objects.clone(), opts.clone()));
        }

        let results = join_all(futures).await;

        for idx in 0..del_objects.len() {
            for (dels, errs) in results.iter() {
                if errs[idx].is_none() && dels[idx].found {
                    del_errs[idx] = None;
                    del_objects[idx] = dels[idx].clone();
                    break;
                }

                if del_errs[idx].is_none() {
                    del_errs[idx] = errs[idx].clone();
                    del_objects[idx] = dels[idx].clone();
                }
            }
        }

        del_objects.iter_mut().for_each(|v| {
            v.object_name = decode_dir_object(&v.object_name);
        });

        (del_objects, del_errs)

        // let mut futures = Vec::with_capacity(objects.len());

        // for obj in objects.iter() {
        //     futures.push(async move {
        //         self.internal_get_pool_info_existing_with_opts(
        //             bucket,
        //             &obj.object_name,
        //             &ObjectOptions {
        //                 no_lock: true,
        //                 ..Default::default()
        //             },
        //         )
        //         .await
        //     });
        // }

        // let results = join_all(futures).await;

        // // let mut jhs = Vec::new();
        // // let semaphore = Arc::new(Semaphore::new(num_cpus::get()));
        // // let pools = Arc::new(self.pools.clone());

        // // for obj in objects.iter() {
        // //     let (semaphore, pools, bucket, object_name, opt) = (
        // //         semaphore.clone(),
        // //         pools.clone(),
        // //         bucket.to_string(),
        // //         obj.object_name.to_string(),
        // //         ObjectOptions::default(),
        // //     );

        // //     let jh = tokio::spawn(async move {
        // //         let _permit = semaphore.acquire().await.unwrap();
        // //         self.internal_get_pool_info_existing_with_opts(pools.as_ref(), &bucket, &object_name, &opt)
        // //             .await
        // //     });
        // //     jhs.push(jh);
        // // }
        // // let mut results = Vec::new();
        // // for jh in jhs {
        // //     results.push(jh.await.unwrap());
        // // }

        // // Record the mapping pool_idx -> object index
        // let mut pool_obj_idx_map = HashMap::new();
        // let mut orig_index_map = HashMap::new();

        // for (i, res) in results.into_iter().enumerate() {
        //     match res {
        //         Ok((pinfo, _)) => {
        //             if let Some(obj) = objects.get(i) {
        //                 if pinfo.object_info.delete_marker && obj.version_id.is_none() {
        //                     del_objects[i] = DeletedObject {
        //                         delete_marker: pinfo.object_info.delete_marker,
        //                         delete_marker_version_id: pinfo.object_info.version_id.map(|v| v.to_string()),
        //                         object_name: decode_dir_object(&pinfo.object_info.name),
        //                         delete_marker_mtime: pinfo.object_info.mod_time,
        //                         ..Default::default()
        //                     };
        //                     continue;
        //                 }

        //                 if !pool_obj_idx_map.contains_key(&pinfo.index) {
        //                     pool_obj_idx_map.insert(pinfo.index, vec![obj.clone()]);
        //                 } else if let Some(val) = pool_obj_idx_map.get_mut(&pinfo.index) {
        //                     val.push(obj.clone());
        //                 }

        //                 if !orig_index_map.contains_key(&pinfo.index) {
        //                     orig_index_map.insert(pinfo.index, vec![i]);
        //                 } else if let Some(val) = orig_index_map.get_mut(&pinfo.index) {
        //                     val.push(i);
        //                 }
        //             }
        //         }
        //         Err(e) => {
        //             if !is_err_object_not_found(&e) && is_err_version_not_found(&e) {
        //                 del_errs[i] = Some(e)
        //             }

        //             if let Some(obj) = objects.get(i) {
        //                 del_objects[i] = DeletedObject {
        //                     object_name: decode_dir_object(&obj.object_name),
        //                     version_id: obj.version_id.map(|v| v.to_string()),
        //                     ..Default::default()
        //                 }
        //             }
        //         }
        //     }
        // }

        // if !pool_obj_idx_map.is_empty() {
        //     for (i, sets) in self.pools.iter().enumerate() {
        //         // Retrieve the object index for a pool idx
        //         if let Some(objs) = pool_obj_idx_map.get(&i) {
        //             // Fetch the corresponding object (should never be None)
        //             // let objs: Vec<ObjectToDelete> = obj_idxs.iter().filter_map(|&idx| objects.get(idx).cloned()).collect();

        //             if objs.is_empty() {
        //                 continue;
        //             }

        //             let (pdel_objs, perrs) = sets.delete_objects(bucket, objs.clone(), opts.clone()).await?;

        //             // Insert simultaneously (should never be None)
        //             let org_indexes = orig_index_map.get(&i).unwrap();

        //             // perrs should follow the same order as obj_idxs
        //             for (i, err) in perrs.into_iter().enumerate() {
        //                 let obj_idx = org_indexes[i];

        //                 if err.is_some() {
        //                     del_errs[obj_idx] = err;
        //                 }

        //                 let mut dobj = pdel_objs.get(i).unwrap().clone();
        //                 dobj.object_name = decode_dir_object(&dobj.object_name);

        //                 del_objects[obj_idx] = dobj;
        //             }
        //         }
        //     }
        // }

        // Ok((del_objects, del_errs))
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            let _ = self.pools[0].add_partial(bucket, object.as_str(), version_id).await;
            return Ok(());
        }

        let opts = ObjectOptions {
            version_id: Some(version_id.to_string()),
            ..Default::default()
        };
        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), &opts)
            .await?;

        let _ = self.pools[idx].add_partial(bucket, object.as_str(), version_id).await;
        Ok(())
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let object = encode_dir_object(object);
        if self.single_pool() {
            return self.pools[0].transition_object(bucket, &object, opts).await;
        }

        //opts.skip_decommissioned = true;
        //opts.no_lock = true;
        let (_, idx) = self.get_latest_accessible_object_info_with_idx(bucket, &object, opts).await?;

        self.pools[idx].transition_object(bucket, &object, opts).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_restore_transitioned_object(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<()> {
        let object = encode_dir_object(object);
        if self.single_pool() {
            return self.pools[0].clone().restore_transitioned_object(bucket, &object, opts).await;
        }

        //opts.skip_decommissioned = true;
        //opts.nolock = true;
        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), opts)
            .await?;

        self.pools[idx]
            .clone()
            .restore_transitioned_object(bucket, &object, opts)
            .await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_put_object_metadata(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);
        if self.single_pool() {
            return self.pools[0].put_object_metadata(bucket, object.as_str(), opts).await;
        }

        let mut opts = opts.clone();
        opts.metadata_chg = true;

        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), &opts)
            .await?;

        self.pools[idx].put_object_metadata(bucket, object.as_str(), &opts).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].get_object_tags(bucket, object.as_str(), opts).await;
        }

        let (oi, _) = self.get_latest_accessible_object_info_with_idx(bucket, &object, opts).await?;
        Ok(oi.user_tags)
    }

    #[instrument(level = "debug", skip(self))]
    pub(super) async fn handle_put_object_tags(
        &self,
        bucket: &str,
        object: &str,
        tags: &str,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].put_object_tags(bucket, object.as_str(), tags, opts).await;
        }

        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), opts)
            .await?;

        self.pools[idx].put_object_tags(bucket, object.as_str(), tags, opts).await
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_delete_object_version(
        &self,
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        force_del_marker: bool,
    ) -> Result<()> {
        check_del_obj_args(bucket, object)?;

        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0]
                .delete_object_version(bucket, object.as_str(), fi, force_del_marker)
                .await;
        }
        Err(StorageError::NotImplemented)
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        let object = encode_dir_object(object);

        if self.single_pool() {
            return self.pools[0].delete_object_tags(bucket, object.as_str(), opts).await;
        }

        let (_, idx) = self
            .get_latest_accessible_object_info_with_idx(bucket, object.as_str(), opts)
            .await?;

        self.pools[idx].delete_object_tags(bucket, object.as_str(), opts).await
    }

    pub(super) async fn handle_verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let get_object_reader = <Self as ObjectIO>::get_object_reader(self, bucket, object, None, HeaderMap::new(), opts).await?;
        // Stream to sink to avoid loading entire object into memory during verification
        let mut reader = get_object_reader.stream;
        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_marker_data_movement_falls_back_when_only_source_pool_has_object() {
        let target = select_data_movement_target_pool(Ok(1), 1, true).unwrap();
        assert_eq!(target, None);
    }

    #[test]
    fn delete_marker_data_movement_falls_back_when_version_does_not_exist_yet() {
        let err = StorageError::ObjectNotFound("bucket".to_string(), "object".to_string());
        let target = select_data_movement_target_pool(Err(err), 1, true).unwrap();
        assert_eq!(target, None);
    }

    #[test]
    fn non_delete_marker_data_movement_keeps_existing_pool() {
        let target = select_data_movement_target_pool(Ok(0), 1, false).unwrap();
        assert_eq!(target, Some(0));
    }

    #[test]
    fn latest_object_access_delete_marker_error_returns_none_for_live_object() {
        let info = ObjectInfo::default();
        let opts = ObjectOptions::default();

        assert!(latest_object_access_delete_marker_error("bucket", "object", &info, &opts).is_none());
    }

    #[test]
    fn latest_object_access_delete_marker_error_returns_not_found_without_version_id() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions::default();

        let err = latest_object_access_delete_marker_error("bucket", "object", &info, &opts)
            .expect("delete marker should stop latest-object reads");

        assert!(crate::error::is_err_object_not_found(&err));
    }

    #[test]
    fn latest_object_access_delete_marker_error_returns_method_not_allowed_for_version_read() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };

        let err = latest_object_access_delete_marker_error("bucket", "object", &info, &opts)
            .expect("delete marker version reads should be rejected");

        assert!(matches!(err, Error::MethodNotAllowed));
    }

    #[test]
    fn latest_object_access_delete_marker_error_returns_not_found_for_delete_marker_lookup() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            delete_marker: true,
            ..Default::default()
        };

        let err = latest_object_access_delete_marker_error("bucket", "object", &info, &opts)
            .expect("delete marker lookup should keep not-found semantics");

        assert!(crate::error::is_err_object_not_found(&err));
    }

    #[test]
    fn resolve_latest_object_access_returns_live_object_and_pool_idx() {
        let info = ObjectInfo::default();
        let opts = ObjectOptions::default();

        let (resolved, idx) = resolve_latest_object_access("bucket", "object", info, 7, &opts).unwrap();

        assert_eq!(idx, 7);
        assert!(!resolved.delete_marker);
    }

    #[test]
    fn resolve_latest_object_access_rejects_delete_marker_without_version_id() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions::default();

        let err = resolve_latest_object_access("bucket", "object", info, 2, &opts).unwrap_err();

        assert!(crate::error::is_err_object_not_found(&err));
    }

    #[test]
    fn resolve_latest_object_access_rejects_delete_marker_version_read() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };

        let err = resolve_latest_object_access("bucket", "object", info, 2, &opts).unwrap_err();

        assert!(matches!(err, Error::MethodNotAllowed));
    }

    #[test]
    fn resolve_decommission_target_pool_idx_result_passthrough_ok() {
        let idx = ECStore::resolve_decommission_target_pool_idx_result(Ok(3), "bucket", "object").unwrap();

        assert_eq!(idx, 3);
    }

    #[test]
    fn resolve_decommission_target_pool_idx_result_wraps_error_context() {
        let err = ECStore::resolve_decommission_target_pool_idx_result(Err(Error::other("boom")), "bucket", "object")
            .expect_err("expected contextual error");
        let rendered = err.to_string();

        assert!(rendered.contains("failed to select decommission target pool"), "{rendered}");
        assert!(rendered.contains("bucket"), "{rendered}");
        assert!(rendered.contains("object"), "{rendered}");
        assert!(rendered.contains("boom"), "{rendered}");
    }

    #[test]
    fn resolve_decommission_tiered_object_result_passthrough_ok() {
        ECStore::resolve_decommission_tiered_object_result(Ok(()), "bucket", "object")
            .expect("successful decommission result should pass through");
    }

    #[test]
    fn resolve_decommission_tiered_object_result_wraps_error_context() {
        let err = ECStore::resolve_decommission_tiered_object_result(Err(Error::other("boom")), "bucket", "object")
            .expect_err("expected contextual error");
        let rendered = err.to_string();

        assert!(rendered.contains("failed to decommission tiered object"), "{rendered}");
        assert!(rendered.contains("bucket"), "{rendered}");
        assert!(rendered.contains("object"), "{rendered}");
        assert!(rendered.contains("boom"), "{rendered}");
    }

    #[test]
    fn version_aware_lookup_opts_enables_version_aware_lookup() {
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };

        let lookup_opts = version_aware_lookup_opts(&opts, true);

        assert!(lookup_opts.no_lock);
        assert!(lookup_opts.metadata_chg);
        assert_eq!(lookup_opts.version_id.as_deref(), Some("vid-1"));
    }

    #[test]
    fn version_aware_lookup_opts_keeps_latest_lookup_for_unversioned_requests() {
        let lookup_opts = version_aware_lookup_opts(&ObjectOptions::default(), true);

        assert!(lookup_opts.no_lock);
        assert!(!lookup_opts.metadata_chg);
        assert!(lookup_opts.version_id.is_none());
    }

    #[test]
    fn data_movement_pool_lookup_opts_enables_version_aware_lookup_and_skip_flags() {
        let opts = ObjectOptions {
            version_id: Some("vid-1".to_string()),
            ..Default::default()
        };

        let lookup_opts = data_movement_pool_lookup_opts(&opts, false);

        assert!(!lookup_opts.no_lock);
        assert!(lookup_opts.metadata_chg);
        assert!(lookup_opts.skip_decommissioned);
        assert!(lookup_opts.skip_rebalancing);
        assert_eq!(lookup_opts.version_id.as_deref(), Some("vid-1"));
    }

    #[test]
    fn data_movement_pool_lookup_opts_keeps_no_lock_for_tiered_moves() {
        let lookup_opts = data_movement_pool_lookup_opts(
            &ObjectOptions {
                version_id: Some("vid-1".to_string()),
                ..Default::default()
            },
            true,
        );

        assert!(lookup_opts.no_lock);
        assert!(lookup_opts.metadata_chg);
        assert!(lookup_opts.skip_decommissioned);
        assert!(lookup_opts.skip_rebalancing);
    }
}
