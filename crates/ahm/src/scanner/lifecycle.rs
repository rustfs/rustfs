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

use rustfs_common::metrics::IlmAction;
use rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc;
use rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::{apply_lifecycle_action, eval_action_from_lifecycle};
use rustfs_ecstore::bucket::metadata_sys::get_object_lock_config;
use rustfs_ecstore::bucket::versioning::VersioningApi as _;
use rustfs_ecstore::store_api::ObjectInfo;
use rustfs_filemeta::{FileMetaVersion, MetaCacheEntry};
use s3s::dto::BucketLifecycleConfiguration as LifecycleConfig;
use s3s::dto::VersioningConfiguration;
use tracing::info;

#[derive(Clone)]
pub struct ScannerItem {
    bucket: String,
    lifecycle: Option<Arc<LifecycleConfig>>,
    versioning: Option<Arc<VersioningConfiguration>>,
}

impl ScannerItem {
    pub fn new(
        bucket: String,
        lifecycle: Option<Arc<LifecycleConfig>>,
        versioning: Option<Arc<VersioningConfiguration>>,
    ) -> Self {
        Self {
            bucket,
            lifecycle,
            versioning,
        }
    }

    pub async fn apply_actions(&mut self, object: &str, mut meta: MetaCacheEntry) -> anyhow::Result<()> {
        info!("apply_actions called for object: {}", object);
        if self.lifecycle.is_none() {
            info!("No lifecycle config for object: {}", object);
            return Ok(());
        }
        info!("Lifecycle config exists for object: {}", object);

        let file_meta = match meta.xl_meta() {
            Ok(meta) => meta,
            Err(e) => {
                tracing::error!("Failed to get xl_meta for {}: {}", object, e);
                return Ok(());
            }
        };

        let latest_version = file_meta.versions.first().cloned().unwrap_or_default();
        let file_meta_version = FileMetaVersion::try_from(latest_version.meta.as_slice()).unwrap_or_default();

        let obj_info = ObjectInfo {
            bucket: self.bucket.clone(),
            name: object.to_string(),
            version_id: latest_version.header.version_id,
            mod_time: latest_version.header.mod_time,
            size: file_meta_version.object.as_ref().map_or(0, |o| o.size),
            user_defined: serde_json::from_slice(file_meta.data.as_slice()).unwrap_or_default(),
            ..Default::default()
        };

        self.apply_lifecycle(&obj_info).await;

        Ok(())
    }

    async fn apply_lifecycle(&mut self, oi: &ObjectInfo) -> (IlmAction, i64) {
        let size = oi.size;
        if self.lifecycle.is_none() {
            return (IlmAction::NoneAction, size);
        }

        let (olcfg, rcfg) = if self.bucket != ".minio.sys" {
            (
                get_object_lock_config(&self.bucket).await.ok(),
                None, // FIXME: replication config
            )
        } else {
            (None, None)
        };

        let lc_evt = eval_action_from_lifecycle(
            self.lifecycle.as_ref().unwrap(),
            olcfg
                .as_ref()
                .and_then(|(c, _)| c.rule.as_ref().and_then(|r| r.default_retention.clone())),
            rcfg.clone(),
            oi,
        )
        .await;

        info!("lifecycle: {} Initial scan: {}", oi.name, lc_evt.action);

        let mut new_size = size;
        match lc_evt.action {
            IlmAction::DeleteVersionAction | IlmAction::DeleteAllVersionsAction | IlmAction::DelMarkerDeleteAllVersionsAction => {
                new_size = 0;
            }
            IlmAction::DeleteAction => {
                if let Some(vcfg) = &self.versioning {
                    if !vcfg.enabled() {
                        new_size = 0;
                    }
                } else {
                    new_size = 0;
                }
            }
            _ => (),
        }

        apply_lifecycle_action(&lc_evt, &LcEventSrc::Scanner, oi).await;
        (lc_evt.action, new_size)
    }
}
