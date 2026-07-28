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

pub struct PolicySys {}

impl PolicySys {
    pub async fn is_allowed(args: &BucketPolicyArgs<'_>) -> bool {
        matches!(Self::try_is_allowed(args).await, Ok(true))
    }

    pub async fn try_is_allowed(args: &BucketPolicyArgs<'_>) -> Result<bool> {
        Self::is_allowed_with_policy(args, Self::get(args.bucket).await).await
    }

    async fn is_allowed_with_policy(args: &BucketPolicyArgs<'_>, policy: Result<BucketPolicy>) -> Result<bool> {
        match policy {
            Ok(policy) => Ok(policy.is_allowed(args).await),
            Err(StorageError::ConfigNotFound) => Ok(args.is_owner),
            Err(err) => Err(err),
        }
    }

    pub async fn get(bucket: &str) -> Result<BucketPolicy> {
        let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
        let bucket_meta_sys = bucket_meta_sys_lock.read().await;

        let (cfg, _) = bucket_meta_sys.get_bucket_policy(bucket).await?;

        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::{PolicySys, StorageError};
    use rustfs_policy::policy::action::{Action, S3Action};
    use rustfs_policy::policy::{BucketPolicy, BucketPolicyArgs};
    use std::collections::HashMap;

    fn args<'a>(
        is_owner: bool,
        groups: &'a Option<Vec<String>>,
        conditions: &'a HashMap<String, Vec<String>>,
    ) -> BucketPolicyArgs<'a> {
        BucketPolicyArgs {
            bucket: "bucket",
            action: Action::S3Action(S3Action::GetObjectAction),
            is_owner,
            account: "account",
            groups,
            conditions,
            object: "object",
        }
    }

    #[tokio::test]
    async fn missing_policy_preserves_owner_and_iam_fallback_semantics() {
        let groups = None;
        let conditions = HashMap::new();
        assert!(
            PolicySys::is_allowed_with_policy(&args(true, &groups, &conditions), Err(StorageError::ConfigNotFound),)
                .await
                .expect("missing policy should preserve owner access")
        );
        assert!(
            !PolicySys::is_allowed_with_policy(&args(false, &groups, &conditions), Err(StorageError::ConfigNotFound),)
                .await
                .expect("missing policy should defer non-owner access to IAM")
        );
    }

    #[tokio::test]
    async fn policy_load_failures_propagate() {
        let groups = None;
        let conditions = HashMap::new();
        let result = PolicySys::is_allowed_with_policy(
            &args(true, &groups, &conditions),
            Err(StorageError::Io(std::io::Error::other("policy read failed"))),
        )
        .await;

        assert!(
            matches!(result, Err(StorageError::Io(ref err)) if err.to_string() == "policy read failed"),
            "policy load failures must propagate instead of granting owner access"
        );
    }
}
