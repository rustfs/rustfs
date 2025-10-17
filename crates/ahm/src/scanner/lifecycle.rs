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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::Result;
use rustfs_common::data_usage::SizeSummary;
use rustfs_common::metrics::IlmAction;
use rustfs_ecstore::bucket::lifecycle::{
    bucket_lifecycle_audit::LcEventSrc,
    bucket_lifecycle_ops::{GLOBAL_ExpiryState, apply_lifecycle_action, eval_action_from_lifecycle},
    lifecycle,
    lifecycle::Lifecycle,
};
use rustfs_ecstore::bucket::metadata_sys::get_object_lock_config;
use rustfs_ecstore::bucket::object_lock::objectlock_sys::{BucketObjectLockSys, enforce_retention_for_deletion};
use rustfs_ecstore::bucket::versioning::VersioningApi;
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::store_api::{ObjectInfo, ObjectToDelete};
use rustfs_filemeta::FileInfo;
use s3s::dto::{BucketLifecycleConfiguration as LifecycleConfig, VersioningConfiguration};
use time::OffsetDateTime;
use tracing::info;

static SCANNER_EXCESS_OBJECT_VERSIONS: AtomicU64 = AtomicU64::new(100);
static SCANNER_EXCESS_OBJECT_VERSIONS_TOTAL_SIZE: AtomicU64 = AtomicU64::new(1024 * 1024 * 1024 * 1024); // 1 TB

#[derive(Clone)]
pub struct ScannerItem {
    pub bucket: String,
    pub object_name: String,
    pub lifecycle: Option<Arc<LifecycleConfig>>,
    pub versioning: Option<Arc<VersioningConfiguration>>,
}

impl ScannerItem {
    pub fn new(
        bucket: String,
        lifecycle: Option<Arc<LifecycleConfig>>,
        versioning: Option<Arc<VersioningConfiguration>>,
    ) -> Self {
        Self {
            bucket,
            object_name: "".to_string(),
            lifecycle,
            versioning,
        }
    }

    pub async fn apply_versions_actions(&self, fivs: &[FileInfo]) -> Result<Vec<ObjectInfo>> {
        let obj_infos = self.apply_newer_noncurrent_version_limit(fivs).await?;
        if obj_infos.len() >= SCANNER_EXCESS_OBJECT_VERSIONS.load(Ordering::SeqCst) as usize {
            // todo
        }

        let mut cumulative_size = 0;
        for obj_info in obj_infos.iter() {
            cumulative_size += obj_info.size;
        }

        if cumulative_size >= SCANNER_EXCESS_OBJECT_VERSIONS_TOTAL_SIZE.load(Ordering::SeqCst) as i64 {
            //todo
        }

        Ok(obj_infos)
    }

    pub async fn apply_newer_noncurrent_version_limit(&self, fivs: &[FileInfo]) -> Result<Vec<ObjectInfo>> {
        let lock_enabled = if let Some(rcfg) = BucketObjectLockSys::get(&self.bucket).await {
            rcfg.mode.is_some()
        } else {
            false
        };
        let _vcfg = BucketVersioningSys::get(&self.bucket).await?;

        let versioned = match BucketVersioningSys::get(&self.bucket).await {
            Ok(vcfg) => vcfg.versioned(&self.object_name),
            Err(_) => false,
        };
        let mut object_infos = Vec::with_capacity(fivs.len());

        if self.lifecycle.is_none() {
            for info in fivs.iter() {
                object_infos.push(ObjectInfo::from_file_info(info, &self.bucket, &self.object_name, versioned));
            }
            return Ok(object_infos);
        }

        let event = self
            .lifecycle
            .as_ref()
            .expect("lifecycle err.")
            .clone()
            .noncurrent_versions_expiration_limit(&lifecycle::ObjectOpts {
                name: self.object_name.clone(),
                ..Default::default()
            })
            .await;
        let lim = event.newer_noncurrent_versions;
        if lim == 0 || fivs.len() <= lim + 1 {
            for fi in fivs.iter() {
                object_infos.push(ObjectInfo::from_file_info(fi, &self.bucket, &self.object_name, versioned));
            }
            return Ok(object_infos);
        }

        let overflow_versions = &fivs[lim + 1..];
        for fi in fivs[..lim + 1].iter() {
            object_infos.push(ObjectInfo::from_file_info(fi, &self.bucket, &self.object_name, versioned));
        }

        let mut to_del = Vec::<ObjectToDelete>::with_capacity(overflow_versions.len());
        for fi in overflow_versions.iter() {
            let obj = ObjectInfo::from_file_info(fi, &self.bucket, &self.object_name, versioned);
            if lock_enabled && enforce_retention_for_deletion(&obj) {
                //if enforce_retention_for_deletion(&obj) {
                /*if self.debug {
                    if obj.version_id.is_some() {
                        info!("lifecycle: {} v({}) is locked, not deleting\n", obj.name, obj.version_id.expect("err"));
                    } else {
                        info!("lifecycle: {} is locked, not deleting\n", obj.name);
                    }
                }*/
                object_infos.push(obj);
                continue;
            }

            if OffsetDateTime::now_utc().unix_timestamp()
                < lifecycle::expected_expiry_time(obj.successor_mod_time.expect("err"), event.noncurrent_days as i32)
                    .unix_timestamp()
            {
                object_infos.push(obj);
                continue;
            }

            to_del.push(ObjectToDelete {
                object_name: obj.name,
                version_id: obj.version_id,
                ..Default::default()
            });
        }

        if !to_del.is_empty() {
            let mut expiry_state = GLOBAL_ExpiryState.write().await;
            expiry_state.enqueue_by_newer_noncurrent(&self.bucket, to_del, event).await;
        }

        Ok(object_infos)
    }

    pub async fn apply_actions(&mut self, oi: &ObjectInfo, _size_s: &mut SizeSummary) -> (bool, i64) {
        let (action, _size) = self.apply_lifecycle(oi).await;

        info!(
            "apply_actions {} {} {:?} {:?}",
            oi.bucket.clone(),
            oi.name.clone(),
            oi.version_id.clone(),
            oi.user_defined.clone()
        );

        // Create a mutable clone if you need to modify fields
        /*let mut oi = oi.clone();
        oi.replication_status = ReplicationStatusType::from(
            oi.user_defined
                .get("x-amz-bucket-replication-status")
                .unwrap_or(&"PENDING".to_string()),
        );
        info!("apply status is: {:?}", oi.replication_status);
        self.heal_replication(&oi, _size_s).await;*/

        if action.delete_all() {
            return (true, 0);
        }

        (false, oi.size)
    }

    async fn apply_lifecycle(&mut self, oi: &ObjectInfo) -> (IlmAction, i64) {
        let size = oi.size;
        if self.lifecycle.is_none() {
            info!("apply_lifecycle: No lifecycle config for object: {}", oi.name);
            return (IlmAction::NoneAction, size);
        }

        info!("apply_lifecycle: Lifecycle config exists for object: {}", oi.name);

        let (olcfg, rcfg) = if self.bucket != ".minio.sys" {
            (
                get_object_lock_config(&self.bucket).await.ok(),
                None, // FIXME: replication config
            )
        } else {
            (None, None)
        };

        info!("apply_lifecycle: Evaluating lifecycle for object: {}", oi.name);

        let lifecycle = match self.lifecycle.as_ref() {
            Some(lc) => lc,
            None => {
                info!("No lifecycle configuration found for object: {}", oi.name);
                return (IlmAction::NoneAction, 0);
            }
        };

        let lc_evt = eval_action_from_lifecycle(
            lifecycle,
            olcfg
                .as_ref()
                .and_then(|(c, _)| c.rule.as_ref().and_then(|r| r.default_retention.clone())),
            rcfg.clone(),
            oi, // Pass oi directly
        )
        .await;

        info!("lifecycle: {} Initial scan: {} (action: {:?})", oi.name, lc_evt.action, lc_evt.action);

        let mut new_size = size;
        match lc_evt.action {
            IlmAction::DeleteVersionAction | IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction => {
                info!("apply_lifecycle: Object {} marked for version deletion, new_size=0", oi.name);
                new_size = 0;
            }
            IlmAction::DeleteAction => {
                info!("apply_lifecycle: Object {} marked for deletion", oi.name);
                if let Some(vcfg) = &self.versioning {
                    if !vcfg.enabled() {
                        info!("apply_lifecycle: Versioning disabled, setting new_size=0");
                        new_size = 0;
                    }
                } else {
                    info!("apply_lifecycle: No versioning config, setting new_size=0");
                    new_size = 0;
                }
            }
            IlmAction::NoneAction => {
                info!("apply_lifecycle: No action for object {}", oi.name);
            }
            _ => {
                info!("apply_lifecycle: Other action {:?} for object {}", lc_evt.action, oi.name);
            }
        }

        if lc_evt.action != IlmAction::NoneAction {
            info!("apply_lifecycle: Applying lifecycle action {:?} for object {}", lc_evt.action, oi.name);
            apply_lifecycle_action(&lc_evt, &LcEventSrc::Scanner, oi).await;
        } else {
            info!("apply_lifecycle: Skipping lifecycle action for object {} as no action is needed", oi.name);
        }

        (lc_evt.action, new_size)
    }
}
