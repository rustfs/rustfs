use super::{
    error::BucketMetadataError,
    get_bucket_metadata_sys,
    policy::bucket_policy::{BucketPolicy, BucketPolicyArgs},
};
use crate::error::Result;
use tracing::warn;

pub struct PolicySys {}

impl PolicySys {
    pub async fn is_allowed(args: &BucketPolicyArgs) -> bool {
        match Self::get(&args.bucket_name).await {
            Ok(cfg) => return cfg.is_allowed(args),
            Err(err) => {
                if !BucketMetadataError::BucketPolicyNotFound.is(&err) {
                    warn!("config get err {:?}", err);
                }
                ()
            }
        }

        args.is_owner
    }
    pub async fn get(bucket: &str) -> Result<BucketPolicy> {
        let bucket_meta_sys_lock = get_bucket_metadata_sys().await;
        let bucket_meta_sys = bucket_meta_sys_lock.write().await;

        let (cfg, _) = bucket_meta_sys.get_bucket_policy(bucket).await?;

        Ok(cfg)
    }
}
