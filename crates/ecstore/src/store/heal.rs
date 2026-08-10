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
use crate::storage_api_contracts::heal::HealOperations as _;
use tracing::trace;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_HEAL: &str = "heal";
const EVENT_HEAL_FORMAT_COMPLETED: &str = "heal_format_completed";
const EVENT_HEAL_OBJECT_STARTED: &str = "heal_object_started";

fn invalid_heal_pool_index(pool_idx: usize, pool_count: usize) -> Error {
    StorageError::InvalidArgument(
        "heal".to_string(),
        "pool".to_string(),
        format!("invalid heal pool index {pool_idx} for {pool_count} pools"),
    )
}

impl ECStore {
    fn get_pools_for_heal_object(&self, opts: &HealOpts) -> Result<Vec<Arc<Sets>>> {
        match opts.pool {
            Some(pool_idx) => Ok(vec![
                self.pools
                    .get(pool_idx)
                    .cloned()
                    .ok_or_else(|| invalid_heal_pool_index(pool_idx, self.pools.len()))?,
            ]),
            None => Ok(self.pools.clone()),
        }
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        let mut r = HealResultItem {
            heal_item_type: HealItemType::Metadata.to_string(),
            detail: "disk-format".to_string(),
            ..Default::default()
        };

        let mut count_no_heal = 0;
        let mut first_error = None;
        for pool in self.pools.iter() {
            let (mut result, err) = pool.heal_format(dry_run).await?;
            if let Some(err) = err {
                match err {
                    StorageError::NoHealRequired => {
                        count_no_heal += 1;
                    }
                    err => {
                        first_error.get_or_insert(err);
                    }
                }
            }
            r.disk_count += result.disk_count;
            r.set_count += result.set_count;
            r.before.drives.append(&mut result.before.drives);
            r.after.drives.append(&mut result.after.drives);
        }
        if let Some(err) = first_error {
            return Ok((r, Some(err)));
        }
        if count_no_heal == self.pools.len() {
            info!(
                event = EVENT_HEAL_FORMAT_COMPLETED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_HEAL,
                dry_run,
                result = "no_heal_required",
                pool_count = self.pools.len(),
                "Heal format completed"
            );
            return Ok((r, Some(StorageError::NoHealRequired)));
        }
        info!(
            event = EVENT_HEAL_FORMAT_COMPLETED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_HEAL,
            dry_run,
            result = "healed_or_inspected",
            disk_count = r.disk_count,
            set_count = r.set_count,
            "Heal format completed"
        );
        Ok((r, None))
    }

    #[instrument(skip(self, targets), fields(pool_index, set_index, target_count = targets.len()))]
    pub async fn heal_replacement_format(
        &self,
        dry_run: bool,
        pool_index: usize,
        set_index: usize,
        targets: &[String],
    ) -> Result<(HealResultItem, Option<Error>)> {
        let pool = self
            .pools
            .get(pool_index)
            .ok_or_else(|| invalid_heal_pool_index(pool_index, self.pools.len()))?;
        let set = pool.disk_set.get(set_index).cloned().ok_or_else(|| {
            StorageError::InvalidArgument(
                "heal".to_string(),
                "set".to_string(),
                format!("invalid heal set index {set_index} for pool {pool_index}"),
            )
        })?;

        set.heal_replacement_format(dry_run, targets).await
    }

    #[instrument(skip(self, targets), fields(pool_index, set_index, target_count = targets.len()))]
    pub async fn replacement_targets_have_version(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        pool_index: usize,
        set_index: usize,
        targets: &[String],
    ) -> Result<bool> {
        let pool = self
            .pools
            .get(pool_index)
            .ok_or_else(|| invalid_heal_pool_index(pool_index, self.pools.len()))?;
        let set = pool.disk_set.get(set_index).cloned().ok_or_else(|| {
            StorageError::InvalidArgument(
                "heal".to_string(),
                "set".to_string(),
                format!("invalid heal set index {set_index} for pool {pool_index}"),
            )
        })?;

        set.replacement_targets_have_version(bucket, object, version_id, targets)
            .await
            .map_err(Into::into)
    }

    #[instrument(skip(self))]
    pub(super) async fn handle_heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        let res = self.peer_sys.heal_bucket(bucket, opts).await?;

        Ok(res)
    }

    #[instrument(level = "trace", skip(self, opts), fields(bucket = %bucket, object = %object, version_id = %version_id))]
    pub(super) async fn handle_heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        trace!(
            event = EVENT_HEAL_OBJECT_STARTED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_HEAL,
            bucket = %bucket,
            object = %object,
            version_id = %version_id,
            remove = opts.remove,
            scan_mode = ?opts.scan_mode,
            "Heal object started"
        );
        let object = encode_dir_object(object);

        let pools = self.get_pools_for_heal_object(opts)?;

        let mut futures = Vec::with_capacity(pools.len());
        for pool in pools.iter() {
            let suspended_complete = {
                let pool_meta = self.pool_meta.read().await;
                pool_meta.is_suspended(pool.pool_idx).then(|| {
                    pool_meta
                        .pools
                        .get(pool.pool_idx)
                        .and_then(|status| status.decommission.as_ref())
                        .is_some_and(|decommission| decommission.complete)
                })
            };
            if let Some(complete) = suspended_complete {
                if opts.pool.is_some() {
                    let _ = pool.get_disks_for_heal_object(&object, opts)?;
                    let err = if complete {
                        StorageError::InvalidArgument(
                            "heal".to_string(),
                            "pool".to_string(),
                            format!("heal pool {} has completed decommission", pool.pool_idx),
                        )
                    } else {
                        Error::SlowDown
                    };
                    return Ok((HealResultItem::default(), Some(err)));
                }
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
        let _ = (bucket, object, opts);
        // Stale multipart reconciliation is already owned by the lifecycle-driven
        // background cleanup path in `bucket_lifecycle_ops.rs`. There is currently
        // no stable object-heal contract that should fan this request out through
        // pool/set storage layers, so keep the placeholder explicit at the ECStore
        // boundary instead of dispatching into lower layers.
        Err(StorageError::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::pools::{PoolDecommissionInfo, PoolStatus};
    use crate::disk::{DiskOption, format::FormatV3, new_disk};
    use crate::layout::endpoints::{Endpoints, PoolEndpoints};
    use crate::store::init_format::{load_format_erasure, save_format_file};

    async fn minimal_heal_pool(pool_idx: usize) -> Arc<Sets> {
        let format = FormatV3::new(1, 1);
        let endpoint_url = format!("http://127.0.0.1:{}/data", 19000 + pool_idx);
        let mut endpoint = Endpoint::try_from(endpoint_url.as_str()).expect("endpoint should parse");
        endpoint.set_pool_index(pool_idx);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);

        Sets::new(
            vec![None],
            &PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: 1,
                endpoints: Endpoints::from(vec![endpoint]),
                cmd_line: String::new(),
                platform: String::new(),
            },
            &format,
            pool_idx,
            0,
        )
        .await
        .expect("minimal pool should build")
    }

    async fn minimal_heal_store() -> ECStore {
        ECStore {
            id: Uuid::new_v4(),
            disk_map: HashMap::new(),
            pools: vec![minimal_heal_pool(0).await, minimal_heal_pool(1).await],
            peer_sys: S3PeerSys {
                clients: Vec::new(),
                pools_count: 2,
            },
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
    async fn heal_object_pool_scope_selects_only_requested_pool() {
        let store = minimal_heal_store().await;
        let pools = store
            .get_pools_for_heal_object(&HealOpts {
                pool: Some(1),
                ..Default::default()
            })
            .expect("requested pool should be selected");

        assert_eq!(pools.len(), 1);
        assert!(Arc::ptr_eq(&pools[0], &store.pools[1]));
    }

    #[tokio::test]
    async fn heal_object_pool_scope_rejects_invalid_pool() {
        let store = minimal_heal_store().await;
        let err = store
            .get_pools_for_heal_object(&HealOpts {
                pool: Some(2),
                ..Default::default()
            })
            .expect_err("out-of-range pool scope must fail closed");

        assert!(
            matches!(err, StorageError::InvalidArgument(_, ref field, ref reason)
                if field == "pool" && reason.contains("invalid heal pool index 2 for 2 pools")),
            "unexpected invalid pool error: {err:?}"
        );
    }

    #[tokio::test]
    async fn scoped_heal_object_defers_when_requested_pool_is_suspended() {
        let mut store = minimal_heal_store().await;
        store.pool_meta = RwLock::new(PoolMeta {
            pools: vec![
                PoolStatus {
                    id: 0,
                    cmd_line: "pool-0".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: None,
                },
                PoolStatus {
                    id: 1,
                    cmd_line: "pool-1".to_string(),
                    last_update: OffsetDateTime::UNIX_EPOCH,
                    decommission: Some(PoolDecommissionInfo {
                        start_time: Some(OffsetDateTime::UNIX_EPOCH),
                        ..Default::default()
                    }),
                },
            ],
            ..Default::default()
        });

        let (_, err) = store
            .handle_heal_object(
                "bucket",
                "object",
                "",
                &HealOpts {
                    pool: Some(1),
                    set: Some(0),
                    ..Default::default()
                },
            )
            .await
            .expect("suspended pool should return a deferred heal result");

        assert!(matches!(err, Some(StorageError::SlowDown)));

        let (_, err) = store
            .handle_heal_object(
                "bucket",
                "object",
                "",
                &HealOpts {
                    set: Some(1),
                    ..Default::default()
                },
            )
            .await
            .expect("unscoped heal should return the active pool result");

        assert!(matches!(err, Some(StorageError::InvalidArgument(_, ref field, _)) if field == "set"));

        let err = store
            .handle_heal_object(
                "bucket",
                "object",
                "",
                &HealOpts {
                    pool: Some(1),
                    set: Some(1),
                    ..Default::default()
                },
            )
            .await
            .expect_err("invalid set scope should fail before suspended pool deferral");

        assert!(matches!(err, StorageError::InvalidArgument(_, ref field, _) if field == "set"));

        {
            let mut pool_meta = store.pool_meta.write().await;
            let decommission = pool_meta.pools[1]
                .decommission
                .as_mut()
                .expect("test pool should have decommission state");
            decommission.complete = true;
        }
        let (_, err) = store
            .handle_heal_object(
                "bucket",
                "object",
                "",
                &HealOpts {
                    pool: Some(1),
                    set: Some(0),
                    ..Default::default()
                },
            )
            .await
            .expect("completed pool should return a terminal heal result");

        assert!(matches!(
            err,
            Some(StorageError::InvalidArgument(_, ref field, ref reason))
                if field == "pool" && reason.contains("completed decommission")
        ));

        for canceled in [false, true] {
            {
                let mut pool_meta = store.pool_meta.write().await;
                let decommission = pool_meta.pools[1]
                    .decommission
                    .as_mut()
                    .expect("test pool should have decommission state");
                decommission.complete = false;
                decommission.failed = !canceled;
                decommission.canceled = canceled;
            }
            let (_, err) = store
                .handle_heal_object(
                    "bucket",
                    "object",
                    "",
                    &HealOpts {
                        pool: Some(1),
                        set: Some(0),
                        ..Default::default()
                    },
                )
                .await
                .expect("clearable terminal pool should return a deferred heal result");

            assert!(matches!(err, Some(StorageError::SlowDown)));
        }
    }

    #[tokio::test]
    async fn handle_heal_format_continues_after_a_pool_error() {
        let canonical_format = FormatV3::new(1, 3);
        let mut foreign_format = canonical_format.clone();
        foreign_format.id = Uuid::new_v4();
        let mut temp_dirs = Vec::new();
        let mut endpoints = Vec::new();
        let mut disks = Vec::new();

        for disk_index in 0..3 {
            let temp_dir = tempfile::tempdir().expect("temporary disk root should be created");
            let mut endpoint = Endpoint::try_from(temp_dir.path().to_str().expect("temporary path should be UTF-8"))
                .expect("temporary endpoint should parse");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_index);
            let disk = new_disk(
                &endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("temporary disk should open");
            let mut disk_format = foreign_format.clone();
            disk_format.erasure.this = foreign_format.erasure.sets[0][disk_index];
            save_format_file(&Some(disk.clone()), &Some(disk_format))
                .await
                .expect("foreign format should be written");
            temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disks.push(Some(disk));
        }

        let pool_endpoints = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 3,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "foreign-format-majority-test".to_string(),
            platform: "test".to_string(),
        };
        let pool = Sets::new(disks, &pool_endpoints, &canonical_format, 0, 1)
            .await
            .expect("test pool should build around the cached canonical format");

        let mut recoverable_format = FormatV3::new(1, 3);
        recoverable_format.id = canonical_format.id;
        let mut recoverable_temp_dirs = Vec::new();
        let mut recoverable_endpoints = Vec::new();
        let mut recoverable_disks = Vec::new();
        let mut unformatted_disk = None;
        for disk_index in 0..3 {
            let temp_dir = tempfile::tempdir().expect("temporary disk root should be created");
            let mut endpoint = Endpoint::try_from(temp_dir.path().to_str().expect("temporary path should be UTF-8"))
                .expect("temporary endpoint should parse");
            endpoint.set_pool_index(1);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_index);
            let disk = new_disk(
                &endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("temporary disk should open");
            if disk_index < 2 {
                let mut disk_format = recoverable_format.clone();
                disk_format.erasure.this = recoverable_format.erasure.sets[0][disk_index];
                save_format_file(&Some(disk.clone()), &Some(disk_format))
                    .await
                    .expect("recoverable format should be written");
            } else {
                unformatted_disk = Some(disk.clone());
            }
            recoverable_temp_dirs.push(temp_dir);
            recoverable_endpoints.push(endpoint);
            recoverable_disks.push(Some(disk));
        }
        let recoverable_pool_endpoints = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 3,
            endpoints: Endpoints::from(recoverable_endpoints),
            cmd_line: "recoverable-format-test".to_string(),
            platform: "test".to_string(),
        };
        let recoverable_pool = Sets::new(recoverable_disks, &recoverable_pool_endpoints, &recoverable_format, 1, 1)
            .await
            .expect("recoverable test pool should build");

        let endpoint_pools = EndpointServerPools::from(vec![pool_endpoints.clone(), recoverable_pool_endpoints.clone()]);
        let store = ECStore {
            id: canonical_format.id,
            disk_map: HashMap::new(),
            pools: vec![pool, recoverable_pool],
            peer_sys: S3PeerSys::new(&endpoint_pools),
            pool_meta: RwLock::new(PoolMeta::default()),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers: RwLock::new(Vec::new()),
            start_gate: Mutex::new(()),
            pool_meta_save_gate: Mutex::new(()),
            ctx: crate::runtime::instance::bootstrap_ctx(),
            bucket_fence_registry: std::sync::Arc::default(),
        };

        let (result, err) = store
            .handle_heal_format(false)
            .await
            .expect("format heal should return the typed pool error");
        assert!(
            matches!(err, Some(StorageError::CorruptedFormat)),
            "foreign format majority must not be downgraded to a successful heal: {err:?}"
        );
        assert_eq!(result.disk_count, 3, "the recoverable pool should still be inspected");
        let healed = load_format_erasure(&unformatted_disk.expect("the unformatted disk handle should be retained"), true)
            .await
            .expect("the later pool should be healed despite the first pool error");
        assert_eq!(healed.erasure.this, recoverable_format.erasure.sets[0][2]);
    }
}
