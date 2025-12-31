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

use super::metadata_sys::get_bucket_metadata_sys;
use crate::error::{Result, StorageError};
use rustfs_policy::policy::{BucketPolicy, BucketPolicyArgs};
use tracing::info;

pub struct PolicySys {}

impl PolicySys {
    pub async fn is_allowed(args: &BucketPolicyArgs<'_>) -> bool {
        match Self::get(args.bucket).await {
            Ok(cfg) => return cfg.is_allowed(args).await,
            Err(err) => {
                if err != StorageError::ConfigNotFound {
                    info!("config get err {:?}", err);
                }
            }
        }

        args.is_owner
    }
    pub async fn get(bucket: &str) -> Result<BucketPolicy> {
        let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
        let bucket_meta_sys = bucket_meta_sys_lock.read().await;

        let (cfg, _) = bucket_meta_sys.get_bucket_policy(bucket).await?;

        Ok(cfg)
    }
}
