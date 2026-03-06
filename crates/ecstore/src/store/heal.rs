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

impl ECStore {
    #[instrument(skip(self))]
    pub(super) async fn handle_heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        info!("heal_format");
        let mut r = HealResultItem {
            heal_item_type: HealItemType::Metadata.to_string(),
            detail: "disk-format".to_string(),
            ..Default::default()
        };

        let mut count_no_heal = 0;
        for pool in self.pools.iter() {
            let (mut result, err) = pool.heal_format(dry_run).await?;
            if let Some(err) = err {
                match err {
                    StorageError::NoHealRequired => {
                        count_no_heal += 1;
                    }
                    _ => {
                        continue;
                    }
                }
            }
            r.disk_count += result.disk_count;
            r.set_count += result.set_count;
            r.before.drives.append(&mut result.before.drives);
            r.after.drives.append(&mut result.after.drives);
        }
        if count_no_heal == self.pools.len() {
            info!("heal format success, NoHealRequired");
            return Ok((r, Some(StorageError::NoHealRequired)));
        }
        info!("heal format success result: {:?}", r);
        Ok((r, None))
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        let res = self.peer_sys.heal_bucket(bucket, opts).await?;

        Ok(res)
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        info!("ECStore heal_object");
        let object = encode_dir_object(object);

        let mut futures = Vec::with_capacity(self.pools.len());
        for pool in self.pools.iter() {
            if self.is_suspended(pool.pool_idx).await {
                continue;
            }
            futures.push(pool.heal_object(bucket, &object, version_id, opts));
        }
        let results = join_all(futures).await;

        let mut errs = Vec::with_capacity(self.pools.len());
        let mut ress = Vec::with_capacity(self.pools.len());

        for res in results.into_iter() {
            match res {
                Ok((result, err)) => {
                    let mut result = result;
                    result.object = decode_dir_object(&result.object);
                    ress.push(result);
                    errs.push(err);
                }
                Err(err) => {
                    errs.push(Some(err));
                    ress.push(HealResultItem::default());
                }
            }
        }

        for (idx, err) in errs.iter().enumerate() {
            if err.is_none() {
                return Ok((ress.remove(idx), None));
            }
        }

        // No pool returned a nil error, return the first non 'not found' error
        for (index, err) in errs.iter().enumerate() {
            return match err {
                Some(err) => {
                    if is_err_object_not_found(err) || is_err_version_not_found(err) {
                        continue;
                    }
                    Ok((ress.remove(index), Some(err.clone())))
                }
                None => Ok((ress.remove(index), None)),
            };
        }

        // At this stage, all errors are 'not found'
        if !version_id.is_empty() {
            return Ok((HealResultItem::default(), Some(Error::FileVersionNotFound)));
        }

        Ok((HealResultItem::default(), Some(Error::FileNotFound)))
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_check_abandoned_parts(&self, bucket: &str, object: &str, opts: &HealOpts) -> Result<()> {
        let object = encode_dir_object(object);
        if self.single_pool() {
            return self.pools[0].check_abandoned_parts(bucket, &object, opts).await;
        }

        let mut errs = Vec::new();
        for pool in self.pools.iter() {
            //TODO: IsSuspended
            if let Err(err) = pool.check_abandoned_parts(bucket, &object, opts).await {
                errs.push(err);
            }
        }

        if !errs.is_empty() {
            return Err(errs[0].clone());
        }

        Ok(())
    }
}
