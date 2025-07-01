use super::{error::BucketMetadataError, metadata_sys::get_bucket_metadata_sys};
use crate::error::Result;
use policy::policy::{BucketPolicy, BucketPolicyArgs};
use tracing::warn;

pub struct PolicySys {}

impl PolicySys {
    pub async fn is_allowed(args: &BucketPolicyArgs<'_>) -> bool {
        match Self::get(args.bucket).await {
            Ok(cfg) => return cfg.is_allowed(args),
            Err(err) => {
                let berr: BucketMetadataError = err.into();
                if berr != BucketMetadataError::BucketPolicyNotFound {
                    warn!("config get err {:?}", berr);
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
