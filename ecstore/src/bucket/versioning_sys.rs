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

use super::{metadata_sys::get_bucket_metadata_sys, versioning::VersioningApi};
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::Result;
use s3s::dto::VersioningConfiguration;
use tracing::warn;

pub struct BucketVersioningSys {}

impl Default for BucketVersioningSys {
    fn default() -> Self {
        Self::new()
    }
}

impl BucketVersioningSys {
    pub fn new() -> Self {
        Self {}
    }
    pub async fn enabled(bucket: &str) -> bool {
        match Self::get(bucket).await {
            Ok(res) => res.enabled(),
            Err(err) => {
                warn!("{:?}", err);
                false
            }
        }
    }

    pub async fn prefix_enabled(bucket: &str, prefix: &str) -> bool {
        match Self::get(bucket).await {
            Ok(res) => res.prefix_enabled(prefix),
            Err(err) => {
                warn!("{:?}", err);
                false
            }
        }
    }

    pub async fn suspended(bucket: &str) -> bool {
        match Self::get(bucket).await {
            Ok(res) => res.suspended(),
            Err(err) => {
                warn!("{:?}", err);
                false
            }
        }
    }

    pub async fn prefix_suspended(bucket: &str, prefix: &str) -> bool {
        match Self::get(bucket).await {
            Ok(res) => res.prefix_suspended(prefix),
            Err(err) => {
                warn!("{:?}", err);
                false
            }
        }
    }

    pub async fn get(bucket: &str) -> Result<VersioningConfiguration> {
        if bucket == RUSTFS_META_BUCKET || bucket.starts_with(RUSTFS_META_BUCKET) {
            return Ok(VersioningConfiguration::default());
        }

        let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
        let bucket_meta_sys = bucket_meta_sys_lock.write().await;

        let (cfg, _) = bucket_meta_sys.get_versioning_config(bucket).await?;

        Ok(cfg)
    }
}
